(in-package #:pg)

(defun unit-continuation-snapshot ()
  (let* ((origin (timeline-encode-history 2 1 96 "origin" nil))
         (history (timeline-encode-history 3 2 256 "later" origin))
         (slot (unit-imported-state-baseline)))
    (logical-slot-copy-import-state-image
     (list :version 1 :binding (make-array 3 :element-type '(unsigned-byte 8)
                                             :initial-contents '(1 2 3))
           :generation 1 :system-id 42 :origin '(2 96)
           :history-target 3 :history history :sealed-end '(320 3 :prefix-end)
           :slots (list (list (wal-replication-slot-name slot) (logical-slot-state-encode slot)))
           :transfers nil
           :roots (list
                   (list 1 42 '(:root-id 11 :base-timeline 1 :base 32 :manifest-digest 101
                                :end 256 :end-digest 201) 2 origin)
                   (list 2 42 '(:root-id 12 :base-timeline 2 :base 160 :manifest-digest 102
                                :end 320 :end-digest 202) 3 history))
           :runs '((1 (96 2 :record-start) (256 2 :prefix-end) 2 2)
                   (2 (256 3 :record-start) (280 3 :prefix-end) 3 3))
           :tail '((280 3 :prefix-end) 3)))))

(cl-spec:defproperty continuation-snapshot-roundtrips-claims
    ((generation (range integer 1 18446744073709551615)) (byte (range integer 0 255)))
  (:about logical-slot-continuation-encode)
  (:trials (:normal 200))
  (let ((snapshot (unit-continuation-snapshot)))
    (setf (getf snapshot :generation) generation
          (aref (getf snapshot :binding) 0) byte)
    (let* ((image (logical-slot-continuation-encode snapshot))
           (decoded (logical-slot-continuation-decode image)))
      (and (logical-slot-copy-committed-image-equal-p snapshot decoded)
           (equalp image (logical-slot-continuation-encode decoded))
           (not (eq (getf snapshot :binding) (getf decoded :binding)))
           (not (eq (fifth (first (getf snapshot :roots)))
                    (fifth (first (getf decoded :roots)))))))))

(cl-spec:defproperty continuation-normalization-keeps-whole-boundary-runs
    ((mode (range integer 0 2)))
  (:about logical-slot-continuation-normalize)
  (:trials (:normal 200))
  (let* ((snapshot (unit-continuation-snapshot))
         (row (first (getf snapshot :slots)))
         (slot (logical-slot-state-decode (second row) (first row)))
         (restart (nth mode '(32 240 280)))
         (owner (if (= mode 2) 3 (if (= mode 1) 2 1))))
    (setf (wal-replication-slot-restart-lsn slot) restart
          (wal-replication-slot-restart-timeline slot) owner
          (wal-replication-slot-confirmed-flush-lsn slot) 300
          (wal-replication-slot-confirmed-timeline slot) 3)
    (let* ((updated (logical-slot-continuation-plan-slot-update
                     snapshot (first row) (logical-slot-state-encode slot)))
           (normalized (logical-slot-continuation-normalize updated))
           (again (logical-slot-continuation-normalize normalized))
           (runs (getf normalized :runs)))
      (and (equalp (logical-slot-continuation-encode normalized)
                   (logical-slot-continuation-encode again))
           (= (length runs) (if (= mode 2) 1 2))
           (= (first (second (first runs))) (if (= mode 2) 256 96))
           (= (getf (third (first (getf normalized :roots))) :base) (if (= mode 2) 160 32))
           (= 32 (wal-replication-slot-restart-lsn
                  (logical-slot-state-decode (second row) (first row))))
           (equalp (getf snapshot :binding) (getf normalized :binding))))))

(cl-spec:defproperty continuation-snapshot-retains-wide-slot-identities
    ((generation (range integer 18446744073709551616 340282366920938463463374607431768211455)))
  (:about logical-slot-continuation-encode)
  (:trials (:normal 200))
  (let* ((snapshot (unit-continuation-snapshot))
         (owner (logxor generation 1))
         (slot (unit-imported-state-baseline :generation generation :owner owner)))
    (setf (getf snapshot :slots)
          (list (list (wal-replication-slot-name slot) (logical-slot-state-encode slot))))
    (let* ((decoded (logical-slot-continuation-decode (logical-slot-continuation-encode snapshot)))
           (row (first (getf decoded :slots)))
           (restored (logical-slot-state-decode (second row) (first row))))
      (and (eql generation (logical-slot-provenance-generation
                            (wal-replication-slot-logical-provenance restored)))
           (eql owner (wal-replication-slot-child-owner restored))))))

(rove:deftest continuation-snapshot-generated-contracts
    (dolist (property '(continuation-snapshot-roundtrips-claims
                        continuation-normalization-keeps-whole-boundary-runs
                        continuation-snapshot-retains-wide-slot-identities))
      (let ((result (cl-spec:result-data
                     (cl-spec:run-property property :seed 90513 :profile :normal))))
        (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
        (rove:ok (= 200 (getf result :trials))))))

(rove:deftest continuation-plan-keeps-multiple-base-identities-and-proof-work
    (let* ((snapshot (unit-continuation-snapshot))
           (before (logical-slot-copy-import-state-image snapshot))
           (plan (logical-slot-continuation-check snapshot)))
      (rove:ok (logical-slot-continuation-plan-p plan))
      (rove:ok (= 96 (logical-slot-continuation-plan-floor plan)))
      (rove:ok (= 2 (length (logical-slot-continuation-plan-root-requests plan))))
      (rove:ok (= 2 (length (logical-slot-continuation-plan-run-requests plan))))
      (rove:ok (logical-slot-continuation-plan-tail-request plan))
      (rove:ok (logical-slot-copy-committed-image-equal-p before snapshot))
      (setf (aref (getf snapshot :binding) 0) 99)
      (rove:ok (= 1 (aref (getf (logical-slot-continuation-plan-snapshot plan) :binding) 0)))
      (let ((missing (logical-slot-copy-import-state-image snapshot)))
        (setf (getf missing :runs)
              '((2 (160 2 :record-start) (256 2 :prefix-end) 2 3)
                (2 (256 3 :record-start) (280 3 :prefix-end) 3 3)))
        (rove:ok (rove:signals (logical-slot-continuation-check missing) 'error)))))

(rove:deftest continuation-snapshot-refuses-siblings-carrier-regression-and-tail-grafts
    (dolist (kind '(:sibling :origin :carrier :root-system :root-base :tail :tail-carrier :position :extra))
      (let ((snapshot (unit-continuation-snapshot)))
        (ecase kind
          (:sibling
           (setf (fourth (second (getf snapshot :roots))) 4
                 (fifth (second (getf snapshot :roots))) (timeline-encode-history 4 1 96 "sibling" nil)))
          (:origin (setf (getf snapshot :origin) '(4 96)))
          (:carrier (setf (fifth (first (getf snapshot :runs))) 3))
          (:root-system (setf (second (first (getf snapshot :roots))) 43))
          (:root-base (setf (getf (third (first (getf snapshot :roots))) :base) 160))
          (:tail (setf (getf snapshot :tail) '((240 2 :prefix-end) 2)))
          (:tail-carrier (setf (second (getf snapshot :tail)) 99))
          (:position (setf (getf snapshot :sealed-end) '(320 2 :prefix-end)))
          (:extra (setf snapshot (append snapshot '(:unknown 1)))))
        (rove:ok (rove:signals (logical-slot-continuation-encode snapshot) 'error)
                 (format nil "~a" kind)))))

(rove:deftest continuation-floor-uses-saved-restart-and-exact-slot-lifetime
    (let* ((snapshot (unit-continuation-snapshot))
           (row (first (getf snapshot :slots)))
           (slot (logical-slot-state-decode (second row) (first row))))
      (setf (wal-replication-slot-confirmed-flush-lsn slot) 300
            (wal-replication-slot-confirmed-timeline slot) 3)
      (let* ((updated (logical-slot-continuation-plan-slot-update
                       snapshot (first row) (logical-slot-state-encode slot)))
             (plan (logical-slot-continuation-check updated)))
        (rove:ok (= 96 (logical-slot-continuation-plan-floor plan)))))
  (dolist (kind '(:generation :owner :p6 :regression))
    (let* ((snapshot (unit-continuation-snapshot))
           (row (first (getf snapshot :slots)))
           (slot (logical-slot-state-decode (second row) (first row))))
      (ecase kind
        (:generation
         (setf slot (unit-imported-state-with-origin
                     slot :provenance (make-logical-slot-provenance 42 1 65 1 65 1))))
        (:owner (setf slot (unit-imported-state-with-origin slot :owner 105)))
        (:p6 (setf slot (unit-imported-state-with-origin slot :owner nil)))
        (:regression (setf (wal-replication-slot-restart-lsn slot) 31)))
      (rove:ok (rove:signals
                (logical-slot-continuation-plan-slot-update
                 snapshot (first row) (logical-slot-state-encode slot)) 'error)))))

(rove:deftest continuation-zero-demand-keeps-host-binding-and-terminal-state
    (dolist (kind '(:absent :lost))
      (let* ((snapshot (unit-continuation-snapshot))
             (row (first (getf snapshot :slots))))
        (ecase kind
          (:absent (setf (getf snapshot :slots) nil))
          (:lost
           (let ((slot (logical-slot-state-decode (second row) (first row))))
             (setf (wal-replication-slot-invalidation-cause slot) :wal-removed
                   (wal-replication-slot-wal-status slot) :lost
                   snapshot (logical-slot-continuation-plan-slot-update
                             snapshot (first row) (logical-slot-state-encode slot))))))
        (let ((normalized (logical-slot-continuation-normalize snapshot)))
          (rove:ok (null (logical-slot-continuation-plan-floor (logical-slot-continuation-check normalized))))
          (rove:ok (null (getf normalized :roots)))
          (rove:ok (null (getf normalized :runs)))
          (rove:ok (null (getf normalized :tail)))
          (rove:ok (equalp (getf normalized :binding) (getf snapshot :binding)))
          (rove:ok (= (length (getf normalized :slots)) (if (eq kind :lost) 1 0)))))))

(rove:deftest continuation-grammar-bounds-cycles-counts-and-wire-corruption
    (dolist (kind '(:cycle :improper :roots :runs :duplicate :token :token-case))
      (let ((snapshot (unit-continuation-snapshot)))
        (ecase kind
          (:cycle (setf (cdr (last snapshot)) snapshot))
          (:improper (setf (cdr (last (getf snapshot :roots))) :bad))
          (:roots (setf (getf snapshot :roots) (make-list 257)))
          (:runs (setf (getf snapshot :runs) (make-list 65537)))
          (:duplicate (setf (getf snapshot :roots)
                            (list (first (getf snapshot :roots)) (first (getf snapshot :roots)))))
          (:token (setf (getf snapshot :transfers) '(( "not-a-copy-token" (96 2 :record-start)))))
          (:token-case (setf (getf snapshot :transfers)
                             '(("0000000000000000000000000000000A" (96 2 :record-start))))))
        (rove:ok (rove:signals (logical-slot-continuation-encode snapshot) 'error))))
  (let* ((image (logical-slot-continuation-encode (unit-continuation-snapshot)))
         (size (length image)))
    (dolist (offset (list 0 16 20 (- size 5) (1- size)))
      (let ((changed (copy-seq image)))
        (setf (aref changed offset) (logxor 1 (aref changed offset)))
        (rove:ok (rove:signals (logical-slot-continuation-decode changed) 'error))))
    (rove:ok (rove:signals (logical-slot-continuation-decode (subseq image 0 (1- size))) 'error))
    (rove:ok (rove:signals
              (logical-slot-continuation-decode
               (concatenate '(vector (unsigned-byte 8)) image #(0))) 'error))))

(rove:deftest continuation-transfer-obligation-survives-last-slot
    (let ((snapshot (unit-continuation-snapshot)))
      (setf (getf snapshot :slots) nil
            (getf snapshot :transfers)
            '(("0000000000000000000000000000000a" (240 2 :prefix-end))
              ("0000000000000000000000000000000b" (280 3 :prefix-end))))
      (let ((normalized (logical-slot-continuation-normalize snapshot)))
        (rove:ok (= 240 (logical-slot-continuation-plan-floor (logical-slot-continuation-check normalized))))
        (rove:ok (= 2 (length (getf normalized :roots))))
        (rove:ok (null (getf normalized :slots))))
      (setf (getf snapshot :transfers) (rest (getf snapshot :transfers)))
      (let ((normalized (logical-slot-continuation-normalize snapshot)))
        (rove:ok (= 280 (logical-slot-continuation-plan-floor (logical-slot-continuation-check normalized))))
        (rove:ok (= 1 (length (getf normalized :roots))))
        (rove:ok (= 160 (getf (third (first (getf normalized :roots))) :base))))))

(rove:deftest continuation-history-lookup-work-is-bounded-before-run-validation
    (let* ((snapshot (unit-continuation-snapshot))
           (history (getf snapshot :history)) (tip 3) (fork 256))
      (dotimes (step 125)
        (incf fork 100)
        (setf history (timeline-encode-history (1+ tip) tip fork "later" history))
        (incf tip))
      (setf (getf snapshot :history-target) tip (getf snapshot :history) history
            (getf snapshot :sealed-end) (list (+ fork 64) tip :prefix-end)
            (getf snapshot :tail) (list (list (+ fork 32) tip :prefix-end) tip)
            (getf snapshot :runs) (make-list 4096 :initial-element (first (getf snapshot :runs))))
      (let ((message (handler-case (progn (logical-slot-continuation-check snapshot) nil)
                       (error (condition) (princ-to-string condition)))))
        (rove:ok (and message (search "work exceeds its profile" message))))))

(defun unit-continuation-repair-envelope (bytes)
  (wal-set-u32 bytes 16 (- (length bytes) 24))
  (wal-set-u32 bytes (- (length bytes) 4) (wal-crc32 bytes 0 (- (length bytes) 4)))
  bytes)

(rove:deftest continuation-decoder-refuses-crc-valid-malformed-frames
    (let* ((snapshot (unit-continuation-snapshot))
           (image (logical-slot-continuation-encode snapshot))
           (row (first (getf snapshot :slots)))
           (slot-count (+ 20 8 8 4 8 4 (length (getf snapshot :binding))
                          4 4 (length (getf snapshot :history)) 13))
           (slot-start (+ slot-count 4 4 (length (first row)) 4)))
      (dolist (kind '(:count :point-tag :generation :inner-slot :trailing))
        (let ((changed
                (if (eq kind :trailing)
                    (concatenate '(vector (unsigned-byte 8))
                                 (subseq image 0 (- (length image) 4)) #(0 0 0 0 0))
                    (copy-seq image))))
          (ecase kind
            (:count (wal-set-u32 changed slot-count #xffffffff))
            (:point-tag (setf (aref changed (1- slot-count)) 0))
            (:generation (wal-set-u64 changed 20 0))
            (:inner-slot
             (let ((offset (+ slot-start (length (second row)) -1)))
               (setf (aref changed offset) (logxor 1 (aref changed offset)))))
            (:trailing nil))
          (unit-continuation-repair-envelope changed)
          (rove:ok (logical-slot-continuation-check-envelope changed))
          (rove:ok (rove:signals (logical-slot-continuation-decode changed) 'error)
                   (format nil "~a" kind))))))

(rove:deftest continuation-normalization-copies-profile-sized-tables-without-recursive-cdr-walks
    (let* ((snapshot (unit-continuation-snapshot))
           (root (first (getf snapshot :roots)))
           (count +logical-slot-continuation-run-limit+)
           (end (+ 96 (* count 32))))
      (setf (getf snapshot :history-target) 2 (getf snapshot :history) (copy-seq (fifth root))
            (getf snapshot :sealed-end) (list end 2 :prefix-end)
            (getf (third root) :end) end
            (getf snapshot :roots) (list root)
            (getf snapshot :runs)
            (loop for start from 96 below end by 32
                  collect (list 1 (list start 2 :record-start) (list (+ start 32) 2 :prefix-end) 2 2))
            (getf snapshot :tail) (list (list end 2 :prefix-end) 2))
      (let ((normalized (logical-slot-continuation-normalize snapshot)))
        (rove:ok (= count (length (getf normalized :runs))))
        (rove:ok (not (eq (first (getf normalized :runs)) (first (getf snapshot :runs)))))
        (rove:ok (= end (first (third (car (last (getf normalized :runs))))))))))
