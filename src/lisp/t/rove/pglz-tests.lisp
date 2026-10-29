;;;; PGLZ stream compatibility and corruption regressions.
(in-package #:pg)

(rove:deftest test-pglz-streams
    (flet ((octets (&rest values)
             (make-array (length values) :element-type '(unsigned-byte 8)
                                         :initial-contents values)))
      ;; Literal followed by an offset-one back reference; the decoder must copy
      ;; overlapping references forward rather than as a detached substring.
      (rove:ok (equalp (pglz-decompress (octets 2 97 2 1) 6)
                       (octets 97 97 97 97 97 97)))
      ;; A match length of 18 uses the extra length byte.
      (rove:ok (equalp (pglz-decompress (octets 2 97 15 1 0) 19)
                       (make-array 19 :element-type '(unsigned-byte 8)
                                      :initial-element 97)))
      (dolist (stream (list (octets 1) (octets 1 0)
                            (octets 1 0 0) (octets 0 97 0)))
        (rove:ok (handler-case (progn (pglz-decompress stream 1) nil)
                   (error () t))))))

(rove:deftest test-pglz-roundtrip
    (dolist (size '(32 126 127 273 1024 4096 10000))
      (let ((data (make-array size :element-type '(unsigned-byte 8))))
        (dotimes (i size)
          (setf (aref data i) (mod (+ (* (mod i 137) 7) (floor i 137)) 256)))
        (let ((compressed (pglz-compress data)))
          (rove:ok (or (null compressed)
                       (equalp data (pglz-decompress compressed size)))))))
  (let* ((data (make-array 10000 :element-type '(unsigned-byte 8)
                                 :initial-element 42))
         (compressed (pglz-compress data)))
    (rove:ok compressed)
    (rove:ok (equalp data (pglz-decompress compressed (length data)))))
  (rove:ok (null (pglz-compress (make-array 31 :element-type '(unsigned-byte 8)
                                               :initial-element 0)))))

(rove:deftest test-pglz-default-threshold
    ;; The stream is exactly floor(33 * 0.75) bytes, which is rejected.
    (let ((data (make-array 33 :element-type '(unsigned-byte 8))))
      (dotimes (i 19) (setf (aref data i) i))
      (loop for i from 19 below 33 do (setf (aref data i) 18))
      (rove:ok (null (pglz-compress data)))))

(rove:deftest test-toast-compressed-payload
    (let* ((raw (make-array 6300 :element-type '(unsigned-byte 8)))
           (state 1))
      ;; Repeat a pseudorandom block while keeping the stored stream multichunk.
      (dotimes (i 2100)
        (setf state (logand #xffffffff (+ (* state 1664525) 1013904223))
              (aref raw i) (if (< i 200) 42 (ldb (byte 8 24) state))
              (aref raw (+ i 2100)) (aref raw i)
              (aref raw (+ i 4200)) (aref raw i)))
      (let ((payload (toast-compressed-payload raw)))
        (rove:ok payload)
        (when payload
          (rove:ok (> (length payload) +toast-oid-chunk-size+))
          (rove:ok (equalp raw (toast-decompress-payload payload (length raw))))
          (rove:ok (handler-case
                       (progn (toast-decompress-payload payload (1- (length raw))) nil)
                     (error () t)))))))

(rove:deftest test-toast-compressed-pointer-validation
    (let* ((toast (make-pg-toast :oid 99))
           (pointer (make-heap-external-varlena :id-width 4 :relation-id 99
                                                :value-id 1 :raw-size 1004
                                                :external-info 50)))
      (multiple-value-bind (stored raw compressed-p)
          (toast-pointer-storage-size toast pointer)
        (rove:ok (and (= stored 50) (= raw 1000) compressed-p)))
      (setf (heap-external-varlena-external-info pointer) (logior (ash 1 30) 50))
      (rove:ok (handler-case (progn (toast-pointer-storage-size toast pointer) nil)
                 (error () t)))
      (setf (heap-external-varlena-external-info pointer) 1001)
      (rove:ok (handler-case (progn (toast-pointer-storage-size toast pointer) nil)
                 (error () t)))))

(rove:deftest test-toast-compressed-header-corruption
    (let* ((raw (make-array 3000 :element-type '(unsigned-byte 8) :initial-element 42))
           (payload (toast-compressed-payload raw))
           (bad (copy-seq payload)))
      (setf (aref bad 3) (logior (aref bad 3) #x40))
      (rove:ok (handler-case
                   (progn (toast-decompress-payload bad (length raw)) nil)
                 (error () t)))
      (rove:ok (handler-case
                   (progn (toast-decompress-payload (subseq payload 0 4) (length raw)) nil)
                 (error () t)))))

(rove:deftest test-inline-compressed-tuple
    (let* ((columns (vector (make-pg-column :type :int2)
                            (make-pg-column :type :text)
                            (make-pg-column :type :int4)))
           (text (make-string 5000 :initial-element #\x))
           (logical (vector 7 text 13))
           (physical (toast-plan-tuple logical columns))
           (tuple (form-heap-tuple physical columns 42 0 0 0 1)))
      (rove:ok (heap-compressed-varlena-p (aref physical 1)))
      (rove:ok (< (length tuple) +toast-tuple-target+))
      (rove:ok (= (length tuple) (heap-tuple-layout physical columns)))
      (multiple-value-bind (values xmin) (deform-heap-tuple tuple columns)
        (rove:ok (= xmin 42))
        (rove:ok (equalp values logical)))))

(rove:deftest test-inline-compression-externalization-choice
    (let* ((columns (vector (make-pg-column :type :bytea)
                            (make-pg-column :type :bytea)))
           (repeated (make-array 5000 :element-type '(unsigned-byte 8) :initial-element 7))
           (random (make-array 3000 :element-type '(unsigned-byte 8)))
           (state 1))
      (dotimes (i (length random))
        (setf state (logand #xffffffff (+ (* state 1664525) 1013904223))
              (aref random i) (ldb (byte 8 24) state)))
      (multiple-value-bind (physical plan) (toast-plan-tuple (vector repeated random) columns)
        (rove:ok (heap-compressed-varlena-p (aref physical 0)))
        (rove:ok (equal plan '(1))))))

(rove:deftest test-external-compression-follows-planner
    (let ((block (make-array 2100 :element-type '(unsigned-byte 8))) (state 1))
      (dotimes (i (length block))
        (setf state (logand #xffffffff (+ (* state 1664525) 1013904223))
              (aref block i) (if (< i 200) 42 (ldb (byte 8 24) state))))
      (let ((value (concatenate '(vector (unsigned-byte 8)) block block block)))
        (dolist (case '((#\x (0)) (#\e nil)))
          (let ((columns (vector (make-pg-column :type :bytea :storage (first case)))))
            (multiple-value-bind (physical selected compressed)
                (toast-plan-tuple (vector value) columns)
              (rove:ok (heap-external-varlena-p (aref physical 0)))
              (rove:ok (equal selected '(0)))
              (rove:ok (equal compressed (second case)))))))))

(rove:deftest test-inline-compressed-corruption
    (let* ((column (make-pg-column :type :bytea))
           (columns (vector column))
           (value (make-array 5000 :element-type '(unsigned-byte 8) :initial-element 5))
           (physical (toast-plan-tuple (vector value) columns))
           (tuple (form-heap-tuple physical columns 42 0 0 0 1))
           (offset (aref (nth-value 1 (heap-tuple-layout physical columns)) 0))
           (bad (copy-seq tuple)))
      (setf (aref bad (+ offset 7)) (logior #x40 (aref bad (+ offset 7))))
      (rove:ok (handler-case (progn (deform-heap-tuple bad columns) nil)
                 (error () t)))
      (setf bad (subseq tuple 0 (1- (length tuple))))
      (rove:ok (handler-case (progn (deform-heap-tuple bad columns) nil)
                 (error () t)))))

(rove:deftest test-toast-snapshot-text-is-external
    (let* ((state 1)
           (text (make-string 3000))
           (columns (vector (make-pg-column :type :text))))
      (dotimes (i (length text))
        (setf state (logand #xffffffff (+ (* state 1664525) 1013904223))
              (char text i) (code-char (+ 97 (mod (ldb (byte 8 24) state) 26)))))
      (multiple-value-bind (physical indexes) (toast-plan-tuple (vector text) columns)
        (rove:ok (equal indexes '(0)))
        (rove:ok (heap-external-varlena-p (aref physical 0))))))
