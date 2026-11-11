(in-package #:pg)

(cl-spec:defproperty logical-slot-generation-roundtrip
    ((generation (range integer 0 340282366920938463463374607431768211455)))
  (:about logical-slot-generation-token logical-slot-parse-generation)
  (:trials (:normal 200))
  (let ((text (logical-slot-generation-token generation)))
    (and (= 32 (length text))
         (every (lambda (char) (find char "0123456789abcdef")) text)
         (= generation (logical-slot-parse-generation text)))))

(rove:deftest logical-slot-generation-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-slot-generation-roundtrip :seed 246))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials)))))

(rove:deftest logical-slot-generation-canonical-boundaries
    (dolist (pair '((0 "00000000000000000000000000000000")
                    (1 "00000000000000000000000000000001")
                    (340282366920938463463374607431768211455
                     "ffffffffffffffffffffffffffffffff")))
      (rove:ok (string= (second pair) (logical-slot-generation-token (first pair))))
      (rove:ok (= (first pair) (logical-slot-parse-generation (second pair)))))
  (dolist (bad '(nil -1 340282366920938463463374607431768211456 1.0 "1"))
    (rove:ok (rove:signals (logical-slot-generation-token bad) 'error))))

(rove:deftest logical-slot-generation-rejects-noncanonical-tokens
    (dolist (bad '(nil 0 "" "0" "0000000000000000000000000000000"
                   "000000000000000000000000000000000"
                   "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
                   "+0000000000000000000000000000000"
                   " 0000000000000000000000000000000"
                   "0000000000000000000000000000000g"))
      (rove:ok (rove:signals (logical-slot-parse-generation bad) 'error))))

(rove:deftest logical-slot-provenance-keeps-legacy-unknown-on
    (let ((proof (make-logical-slot-provenance
                  18446744073709551615 0 18446744073709551615 4294967295)))
      (rove:ok (logical-slot-provenance-valid-p proof 18446744073709551615))
      (rove:ok (= 18446744073709551615 (logical-slot-provenance-system-id proof)))
      (rove:ok (zerop (logical-slot-provenance-generation proof)))
      (rove:ok (= 4294967295 (logical-slot-provenance-birth-timeline proof)))
      (rove:ok (null (logical-slot-provenance-on-start-lsn proof)))
      (rove:ok (null (logical-slot-provenance-on-start-timeline proof)))
      (rove:ok (not (logical-slot-provenance-valid-p nil 65)))))

(rove:deftest logical-slot-provenance-validates-identity-and-interval
    (let ((proof (make-logical-slot-provenance 42 17 161 3 65 1)))
      (rove:ok (logical-slot-provenance-valid-p proof 161))
      (rove:ok (= 65 (logical-slot-provenance-on-start-lsn proof)))
      (rove:ok (= 1 (logical-slot-provenance-on-start-timeline proof)))
      (rove:ok (not (logical-slot-provenance-valid-p proof 64))))
  (dolist (args '((-1 0 65 1) (18446744073709551616 0 65 1)
                  (42 -1 65 1) (42 340282366920938463463374607431768211456 65 1)
                  (42 0 -1 1) (42 0 18446744073709551616 1)
                  (42 0 65 0) (42 0 65 4294967296)
                  (42 0 65 1 65 nil) (42 0 65 1 nil 1)
                  (42 0 65 1 32 1) (42 0 65 1 66 1)
                  (42 0 65 1 65 2)))
    (rove:ok (rove:signals (apply #'make-logical-slot-provenance args) 'error))))

(cl-spec:defproperty logical-slot-provenance-preserves-full-width
    ((system-id (range integer 0 18446744073709551615))
     (generation (range integer 0 340282366920938463463374607431768211455))
     (birth (range integer 33 18446744073709551615))
     (timeline (range integer 1 4294967295)))
  (:about make-logical-slot-provenance logical-slot-provenance-valid-p)
  (:trials (:normal 200))
  (let ((proof (make-logical-slot-provenance system-id generation birth timeline birth timeline)))
    (and (logical-slot-provenance-valid-p proof birth)
         (= system-id (logical-slot-provenance-system-id proof))
         (= generation (logical-slot-provenance-generation proof))
         (= timeline (logical-slot-provenance-birth-timeline proof))
         (= birth (logical-slot-provenance-on-start-lsn proof))
         (= timeline (logical-slot-provenance-on-start-timeline proof)))))

(rove:deftest logical-slot-provenance-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-slot-provenance-preserves-full-width :seed 247))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials)))))

(rove:deftest logical-slot-restart-selection-keeps-position-role-at-fork
    (let ((history (timeline-parse-history 2 (format nil "1~C0/80~Cfork~%" #\Tab #\Tab))))
      (rove:ok (= 1 (timeline-at-point 127 history)))
      (rove:ok (= 2 (timeline-at-point 128 history)))
      (rove:ok (equal '(128 1 :prefix-end)
                      (multiple-value-list (logical-slot-select-restart-position 128 1 nil))))
      (rove:ok (equal '(128 2 :record-start)
                      (multiple-value-list
                       (logical-slot-select-restart-position 128 1 '((128 . 2))))))
      (rove:ok (equal '(64 1 :record-start)
                      (multiple-value-list
                       (logical-slot-select-restart-position 128 1 '((128 . 2) (64 . 1))))))
      (rove:ok (equal '(128 1 :prefix-end)
                      (multiple-value-list
                       (logical-slot-select-restart-position 128 1 '((160 . 2))))))))

(rove:deftest logical-slot-restart-selection-rejects-conflicting-minimum
    (rove:ok (rove:signals
              (logical-slot-select-restart-position 128 1 '((64 . 1) (64 . 2))) 'error))
  (dolist (args '((-1 1 nil) (18446744073709551616 1 nil) (65 0 nil)
                  (65 4294967296 nil) (65 1 ((-1 . 1))) (65 1 ((0 . 0)))
                  (65 1 ((0 . 4294967296))) (65 1 ((0 1))) (65 1 (nil))))
    (rove:ok (rove:signals (apply #'logical-slot-select-restart-position args) 'error))))

(cl-spec:defproperty logical-slot-restart-selection-prefers-pending-tie
    ((boundary (range integer 0 18446744073709551615))
     (parent (range integer 1 4294967294)))
  (:about logical-slot-select-restart-position)
  (:trials (:normal 200))
  (let ((child (1+ parent)))
    (and (equal (list boundary parent :prefix-end)
                (multiple-value-list (logical-slot-select-restart-position boundary parent nil)))
         (equal (list boundary child :record-start)
                (multiple-value-list
                 (logical-slot-select-restart-position boundary parent
                                                       (list (cons boundary child)
                                                             (cons boundary child))))))))

(rove:deftest logical-slot-restart-selection-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-slot-restart-selection-prefers-pending-tie :seed 248))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials)))))

(defun unit-logical-provenance-slot (&key (provenance t) (known-on t) (cause :none)
                                       (generation 0) (system-id 42) (end 80) (timeline 1)
                                       (plugin "pgoutput") (profile :text))
  (let* ((birth (- end 15))
         (slot (make-wal-replication-slot
                :name "fixture" :database-oid 7 :restart-lsn (- end 48)
                :consistent-lsn birth :confirmed-flush-lsn end :plugin plugin
                :logical-provenance (when provenance
                                      (make-logical-slot-provenance
                                       system-id generation birth timeline
                                       (when known-on birth) (when known-on timeline)))
                :restart-timeline (when provenance timeline)
                :confirmed-timeline (when provenance timeline)
                :invalidation-cause cause
                :wal-status (if (member cause '(nil :none)) :reserved :lost))))
    (wal-slot-set-initial-profile slot profile)
    slot))

(defun unit-logical-slot-state-fields-image (fields)
  (let ((prefix (format nil "~{~a~^ ~}" fields)))
    (string->bytes (format nil "~a ~8,'0X~%" prefix (wal-crc32 (string->bytes prefix))))))

(rove:deftest logical-slot-state-known-provenance-golden
    (let* ((slot (unit-logical-provenance-slot))
           (bytes (wal-encode-replication-slot slot))
           (decoded (wal-decode-replication-slot-image bytes "fixture")))
      (rove:ok (string= (format nil "PGSLOT6 fixture 7 32 65 80 pgoutput text 42 00000000000000000000000000000000 1 65 1 1 1 none B6AA5E5B~%")
                        (bytes->string bytes)))
      (rove:ok (equalp bytes (wal-encode-replication-slot decoded)))
      (rove:ok (zerop (logical-slot-provenance-generation
                       (wal-replication-slot-logical-provenance decoded))))
      (rove:ok (= 1 (wal-replication-slot-restart-timeline decoded)))
      (rove:ok (= 1 (wal-replication-slot-confirmed-timeline decoded)))
      (rove:ok (eq :none (logical-slot-effective-invalidation-cause decoded)))))

(rove:deftest logical-slot-state-legacy-cause-does-not-invent-provenance
    (let* ((slot (unit-logical-provenance-slot :provenance nil :cause :wal-level-insufficient))
           (bytes (wal-encode-replication-slot slot))
           (decoded (wal-decode-replication-slot-image bytes "fixture")))
      (rove:ok (string= (format nil "PGSLOT6 fixture 7 32 65 80 pgoutput text - - - - - - - wal_level_insufficient EF61B905~%")
                        (bytes->string bytes)))
      (rove:ok (null (wal-replication-slot-logical-provenance decoded)))
      (rove:ok (null (wal-replication-slot-restart-timeline decoded)))
      (rove:ok (null (wal-replication-slot-confirmed-timeline decoded)))
      (rove:ok (= 32 (wal-replication-slot-restart-lsn decoded)))
      (rove:ok (eq :lost (wal-replication-slot-wal-status decoded)))
      (rove:ok (eq :wal-level-insufficient (logical-slot-effective-invalidation-cause decoded)))
      (rove:ok (equalp bytes (wal-encode-replication-slot decoded)))))

(rove:deftest logical-slot-state-legacy-save-bytes-stay-unchanged
    (dolist (case '((nil :reserved "reserved" "78E66DEF")
                    (:wal-removed :lost "lost" "113AABBE")))
      (let ((slot (unit-logical-provenance-slot :provenance nil :cause (first case))))
        (setf (wal-replication-slot-wal-status slot) (second case))
        (rove:ok (string= (format nil "PGSLOT4 fixture 7 32 65 80 ~a pgoutput ~a~%"
                                  (third case) (fourth case))
                          (bytes->string (wal-encode-replication-slot slot)))))))

(rove:deftest logical-slot-state-unknown-on-and-binary-profile-roundtrip
    (dolist (profile '(:text :binary-bytea))
      (let* ((slot (unit-logical-provenance-slot :known-on nil :plugin "pg_lisp" :profile profile))
             (bytes (wal-encode-replication-slot slot))
             (decoded (wal-decode-replication-slot-image bytes "fixture"))
             (provenance (wal-replication-slot-logical-provenance decoded)))
        (rove:ok (eq profile (wal-slot-profile decoded)))
        (rove:ok (null (logical-slot-provenance-on-start-lsn provenance)))
        (rove:ok (null (logical-slot-provenance-on-start-timeline provenance)))
        (rove:ok (equalp bytes (wal-encode-replication-slot decoded))))))

(rove:deftest logical-slot-state-rejects-corrupt-and-partial-fields
    (let* ((bytes (wal-encode-replication-slot (unit-logical-provenance-slot)))
           (fields (butlast (wal-split-fields (string-right-trim '(#\Newline) (bytes->string bytes))))))
      (dotimes (i (length bytes))
        (let ((bad (copy-seq bytes)))
          (setf (aref bad i) (logxor 1 (aref bad i)))
          (rove:ok (rove:signals (wal-decode-replication-slot-image bad "fixture") 'error))))
      (dolist (item '((2 "0") (3 "032") (3 "+32") (4 "81") (7 "binary-bytea")
                      (8 "-") (9 "-") (9 "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                      (10 "0") (10 "4294967296") (11 "66") (12 "2")
                      (13 "-") (14 "-") (15 "unknown")))
        (let ((bad (copy-list fields)))
          (setf (nth (first item) bad) (second item))
          (rove:ok (rove:signals
                    (wal-decode-replication-slot-image (unit-logical-slot-state-fields-image bad) "fixture")
                    'error))))
      (rove:ok (rove:signals (wal-decode-replication-slot-image bytes "other") 'error))
      (rove:ok (rove:signals (wal-decode-replication-slot-image (subseq bytes 0 (1- (length bytes))) "fixture") 'error))))

(cl-spec:defproperty logical-slot-state-detached-preserves-ahead-positions
    ((end (range integer 80 18446744073709551615))
     (generation (range integer 0 340282366920938463463374607431768211455))
     (timeline (range integer 1 4294967295)))
  (:about wal-encode-replication-slot wal-decode-replication-slot-image)
  (:trials (:normal 100))
  (let* ((slot (unit-logical-provenance-slot :end end :generation generation :timeline timeline))
         (bytes (wal-encode-replication-slot slot))
         (decoded (wal-decode-replication-slot-image bytes "fixture"))
         (*wal-durable-lsn* 0))
    (and (equalp bytes (wal-encode-replication-slot decoded))
         (= end (wal-replication-slot-confirmed-flush-lsn decoded))
         (= generation (logical-slot-provenance-generation
                        (wal-replication-slot-logical-provenance decoded)))
         (handler-case (progn (wal-decode-replication-slot bytes "fixture") nil)
           (error () t)))))

(rove:deftest logical-slot-state-detached-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-slot-state-detached-preserves-ahead-positions :seed 249))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 100 (getf data :trials)))))

(rove:deftest logical-slot-state-restart-start-can-follow-confirmed-timeline-at-fork
    (let ((slot (unit-logical-provenance-slot :end 128)))
      (setf (wal-replication-slot-restart-lsn slot) 128
            (wal-replication-slot-restart-timeline slot) 2)
      (let* ((bytes (wal-encode-replication-slot slot))
             (decoded (wal-decode-replication-slot-image bytes "fixture")))
        (rove:ok (= 128 (wal-replication-slot-restart-lsn decoded)))
        (rove:ok (= 2 (wal-replication-slot-restart-timeline decoded)))
        (rove:ok (= 1 (wal-replication-slot-confirmed-timeline decoded)))
        (rove:ok (equalp bytes (wal-encode-replication-slot decoded))))))

(rove:deftest logical-slot-state-unconfirmed-private-copy-roundtrip
    (dolist (profile '(:text :binary-bytea))
      (dolist (cause '(:none :wal-removed :wal-level-insufficient))
        (let ((slot (unit-logical-provenance-slot :plugin "pg_lisp" :profile profile :cause cause)))
          (setf (wal-replication-slot-confirmed-flush-lsn slot) nil
                (wal-replication-slot-confirmed-timeline slot) nil)
          (let* ((bytes (wal-encode-replication-slot slot))
                 (decoded (wal-decode-replication-slot-image bytes "fixture")))
            (rove:ok (null (wal-replication-slot-confirmed-flush-lsn decoded)))
            (rove:ok (null (wal-replication-slot-confirmed-timeline decoded)))
            (rove:ok (eq cause (logical-slot-effective-invalidation-cause decoded)))
            (rove:ok (eq profile (wal-slot-profile decoded)))
            (rove:ok (equalp bytes (wal-encode-replication-slot decoded))))))))

(rove:deftest logical-slot-state-rejects-cause-mirror-and-kind-mismatch
    (let ((slot (unit-logical-provenance-slot)))
      (setf (wal-replication-slot-invalidation-cause slot) nil)
      (rove:ok (rove:signals (wal-encode-replication-slot slot) 'error)))
  (let ((slot (unit-logical-provenance-slot :cause :wal-level-insufficient)))
    (setf (wal-replication-slot-wal-status slot) :reserved)
    (rove:ok (rove:signals (wal-encode-replication-slot slot) 'error)))
  (let ((slot (make-wal-replication-slot :name "physical" :kind :physical :plugin nil
                                         :system-id 42 :restart-lsn 32 :restart-timeline 1)))
    (rove:ok (physical-slot-encode slot))
    (setf (wal-replication-slot-invalidation-cause slot) :none)
    (rove:ok (rove:signals (physical-slot-encode slot) 'error))
    (setf (wal-replication-slot-invalidation-cause slot) nil
          (wal-replication-slot-confirmed-timeline slot) 1)
    (rove:ok (rove:signals (physical-slot-encode slot) 'error)))
  (let ((slot (make-wal-replication-slot
               :name "physical" :kind :physical :plugin nil :system-id 42
               :restart-lsn 32 :restart-timeline 1
               :logical-provenance (make-logical-slot-provenance 42 0 65 1))))
    (rove:ok (rove:signals (physical-slot-encode slot) 'error))))

(rove:deftest logical-slot-state-zero-identity-values-remain-present
    (let* ((slot (unit-logical-provenance-slot :system-id 0 :generation 0))
           (bytes (wal-encode-replication-slot slot))
           (fields (wal-split-fields (string-right-trim '(#\Newline) (bytes->string bytes))))
           (decoded (wal-decode-replication-slot-image bytes "fixture"))
           (provenance (wal-replication-slot-logical-provenance decoded)))
      (rove:ok (string= "0" (nth 8 fields)))
      (rove:ok (string= "00000000000000000000000000000000" (nth 9 fields)))
      (rove:ok provenance)
      (rove:ok (zerop (logical-slot-provenance-system-id provenance)))
      (rove:ok (zerop (logical-slot-provenance-generation provenance)))
      (rove:ok (equalp bytes (wal-encode-replication-slot decoded)))))
