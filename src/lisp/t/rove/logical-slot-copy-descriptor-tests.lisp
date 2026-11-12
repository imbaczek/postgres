(in-package #:pg)

(defun copy-descriptor-test-slot (generation)
  (let ((slot
          (make-wal-replication-slot
           :name "alpha" :database-oid 1 :plugin "pgoutput"
           :restart-lsn 33 :consistent-lsn 65 :confirmed-flush-lsn 80
           :restart-timeline 1 :confirmed-timeline 1 :invalidation-cause :none
           :logical-provenance (make-logical-slot-provenance 42 generation 65 1 65 1))))
    (wal-slot-set-initial-profile slot :text)
    (logical-slot-state-encode slot)))

(defun copy-descriptor-test-value (generation salt)
  ;; These opaque blobs exercise framing only. They are not source/control proof.
  (make-logical-slot-copy-descriptor
   :copy-id (logical-slot-generation-token generation) :manifest-digest salt
   :source-control (string->bytes "source") :base-control (string->bytes "base")
   :recovery-control (string->bytes "recovery") :history (string->bytes "")
   :slots (list (list "alpha" (copy-descriptor-test-slot generation)))
   :wal-pieces (list (list 1 0 128 salt 32 128)
                     (list 2 1 96 salt +wal-segment-size+ (+ +wal-segment-size+ 96)))))

(cl-spec:defgenerator copy-descriptor-arguments ()
  (list (copy-descriptor-test-value (random (ash 1 128)) (random (ash 1 64)))))

(cl-spec:defspec-function logical-slot-copy-descriptor-encode
    (:args (descriptor (type logical-slot-copy-descriptor)))
  (:args-generator copy-descriptor-arguments)
  (:returns (type (vector (unsigned-byte 8))))
  (:post (and (<= (length result) +logical-slot-copy-descriptor-maximum-bytes+)
              (equalp descriptor (logical-slot-copy-descriptor-decode result)))))

(cl-spec:defproperty copy-descriptor-preserves-exact-images
    ((generation (range integer 0 340282366920938463463374607431768211455))
     (salt (range integer 0 18446744073709551615)))
  (:about logical-slot-copy-descriptor-encode logical-slot-copy-descriptor-decode)
  (:trials (:normal 200))
  (let* ((value (copy-descriptor-test-value generation salt))
         (image (logical-slot-copy-descriptor-encode value))
         (before (copy-seq image))
         (decoded (logical-slot-copy-descriptor-decode image)))
    (and (equalp value decoded) (equalp before image)
         (equalp image (logical-slot-copy-descriptor-encode decoded))
         (not (eq (logical-slot-copy-descriptor-source-control value)
                  (logical-slot-copy-descriptor-source-control decoded))))))

(rove:deftest copy-descriptor-generated-contracts
    (let ((function-result
            (cl-spec:result-data
             (cl-spec:check-function 'logical-slot-copy-descriptor-encode
                                     :seed 2305 :trials 200)))
          (property-result
            (cl-spec:result-data
             (cl-spec:run-property 'copy-descriptor-preserves-exact-images :seed 2306))))
      (rove:ok (eq :passed (getf function-result :status)) (format nil "~s" function-result))
      (rove:ok (= 200 (getf function-result :trials)))
      (rove:ok (eq :passed (getf property-result :status)) (format nil "~s" property-result))
      (rove:ok (= 200 (getf property-result :trials)))))

(defun copy-descriptor-test-rechecksum (bytes)
  (wal-set-u32 bytes (- (length bytes) 4) 0)
  (wal-set-u32 bytes (- (length bytes) 4) (wal-crc32 bytes))
  bytes)

(rove:deftest copy-descriptor-rejects-damaged-envelope-and-trailing-data
    (let* ((value (copy-descriptor-test-value 0 1))
           (image (logical-slot-copy-descriptor-encode value)))
      (dolist (offset (list 0 15 16 20 (1- (length image))))
        (let ((copy (copy-seq image)))
          (setf (aref copy offset) (logxor 1 (aref copy offset)))
          (rove:ok (rove:signals (logical-slot-copy-descriptor-decode copy) 'error))))
      (dolist (size (list 0 15 20 23 (1- (length image))))
        (rove:ok (rove:signals (logical-slot-copy-descriptor-decode (subseq image 0 size)) 'error)))
      (let ((extra (make-array (1+ (length image)) :element-type '(unsigned-byte 8)
                                                   :initial-element 0)))
        (replace extra image)
        (rove:ok (rove:signals (logical-slot-copy-descriptor-decode extra) 'error)))))

(rove:deftest copy-descriptor-rejects-invalid-inventory-and-fields
    (let ((value (copy-descriptor-test-value 0 1)))
      (dolist (replacement
               (list nil '(("UPPER" #())) '(("alpha" #()))
                     (append (logical-slot-copy-descriptor-slots value)
                             (logical-slot-copy-descriptor-slots value))))
        (let ((candidate (copy-logical-slot-copy-descriptor value)))
          (setf (logical-slot-copy-descriptor-slots candidate) replacement)
          ;; An empty logical inventory is valid, unlike malformed/duplicate entries.
          (if (null replacement)
              (rove:ok (logical-slot-copy-descriptor-encode candidate))
              (rove:ok (rove:signals (logical-slot-copy-descriptor-encode candidate) 'error)))))
      (dolist (pieces '(((0 0 128 1 32 128)) ((1 0 0 1 0 0))
                        ((1 0 128 -1 32 128)) ((1 0 128 1 129 130))
                        ((1 0 128 1 64 32)) ((1 0 128 1 32 128 7))
                        ((2 0 128 1 32 128) (1 0 128 1 32 128))
                        ((1 0 128 1 32 128) (1 0 128 1 32 128))))
        (let ((candidate (copy-logical-slot-copy-descriptor value)))
          (setf (logical-slot-copy-descriptor-wal-pieces candidate) pieces)
          (rove:ok (rove:signals (logical-slot-copy-descriptor-encode candidate) 'error))))
      (dolist (id '("" "0" "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"))
        (let ((candidate (copy-logical-slot-copy-descriptor value)))
          (setf (logical-slot-copy-descriptor-copy-id candidate) id)
          (rove:ok (rove:signals (logical-slot-copy-descriptor-encode candidate) 'error))))))

(rove:deftest copy-descriptor-decodes-ahead-of-local-replay-without-admitting
    (let* ((value (copy-descriptor-test-value 0 1))
           (slot (logical-slot-state-decode
                  (second (first (logical-slot-copy-descriptor-slots value))) "alpha"))
           (before *wal-replication-slots*)
           (position (1- (ash 1 64))))
      (setf (wal-replication-slot-consistent-lsn slot) position
            (wal-replication-slot-restart-lsn slot) position
            (wal-replication-slot-confirmed-flush-lsn slot) position)
      (setf (logical-slot-copy-descriptor-slots value)
            (list (list "alpha" (logical-slot-state-encode slot))))
      (let* ((image (logical-slot-copy-descriptor-encode value))
             (decoded (logical-slot-copy-descriptor-decode image))
             (restored (logical-slot-state-decode
                        (second (first (logical-slot-copy-descriptor-slots decoded))) "alpha")))
        (rove:ok (= position (wal-replication-slot-restart-lsn restored)))
        (rove:ok (eq before *wal-replication-slots*))
        (rove:ok (equalp image (logical-slot-copy-descriptor-encode decoded))))))

(rove:deftest copy-descriptor-independent-little-endian-golden-frame
    ;; Python struct and a scalar Castagnoli oracle produced these expected bytes.
    (let* ((descriptor
             (make-logical-slot-copy-descriptor
              :copy-id "00000000000000000000000000000000"
              :manifest-digest 18446744073709551615
              :source-control (make-array 1 :element-type '(unsigned-byte 8) :initial-element 1)
              :base-control (make-array 1 :element-type '(unsigned-byte 8) :initial-element 2)
              :recovery-control (make-array 1 :element-type '(unsigned-byte 8) :initial-element 3)
              :history (make-array 0 :element-type '(unsigned-byte 8))))
           (expected
             (make-array
              91 :element-type '(unsigned-byte 8)
              :initial-contents
              '(80 71 76 79 71 73 67 65 76 67 79 80 89 49 0 0 67 0 0 0
                48 48 48 48 48 48 48 48 48 48 48 48 48 48 48 48
                48 48 48 48 48 48 48 48 48 48 48 48 48 48 48 48
                255 255 255 255 255 255 255 255 1 0 0 0 1 1 0 0 0 2
                1 0 0 0 3 0 0 0 0 0 0 0 0 0 0 0 0 104 244 7 88))))
      (rove:ok (equalp expected (logical-slot-copy-descriptor-encode descriptor)))
      (rove:ok (equalp descriptor (logical-slot-copy-descriptor-decode expected)))
      (dolist (offset '(60 79 83))
        (let ((damaged (copy-seq expected)))
          (wal-set-u32 damaged offset 4294967295)
          (copy-descriptor-test-rechecksum damaged)
          (rove:ok (rove:signals (logical-slot-copy-descriptor-decode damaged) 'error))))
      (let ((damaged (copy-seq expected)))
        (setf (aref damaged 20) (char-code #\A))
        (copy-descriptor-test-rechecksum damaged)
        (rove:ok (rove:signals (logical-slot-copy-descriptor-decode damaged) 'error)))))

(rove:deftest copy-descriptor-rejects-cyclic-and-improper-caller-lists
    (let* ((descriptor (copy-descriptor-test-value 0 1))
           (cycle (copy-list (logical-slot-copy-descriptor-slots descriptor))))
      (setf (cdr cycle) cycle)
      (dolist (value (list cycle (cons (car cycle) 1)))
        (setf (logical-slot-copy-descriptor-slots descriptor) value)
        (rove:ok (rove:signals (logical-slot-copy-descriptor-encode descriptor) 'error)))))
