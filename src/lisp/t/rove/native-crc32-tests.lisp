(in-package #:pg)

#+x86-64
(defun unit-crc32-bit-reference-seeded (bytes start end seed)
  (let ((crc seed))
    (loop for index from start below end
          do (setf crc (logxor crc (aref bytes index)))
             (dotimes (bit 8)
               (setf crc (if (oddp crc)
                             (logxor (ash crc -1) #xedb88320)
                             (ash crc -1)))))
    (logxor crc #xffffffff)))

#+x86-64
(setf (gethash '%wal-crc32-ieee-octets *unit-hot-probes*)
      (let ((bytes (make-array 1100 :element-type '(unsigned-byte 8) :initial-element 173)))
        (loop for seed in '(0 #xffffffff #x80000001)
              append (loop for size in (if *native-ieee-crc32-available-p*
                                           '(0 1 15 16 17 63 64 65 127 128 129 255 256 257 1024)
                                           '(0 1 15))
                           collect (list bytes 3 (+ 3 size) *wal-crc32-table*
                                         *native-ieee-crc32-constants* seed)))))

#+x86-64
(rove:deftest native-crc32-seeded-folds-match-bit-reference-and-preserve-inputs
    (if *native-ieee-crc32-available-p*
        (let* ((bytes (make-array 8300 :element-type '(unsigned-byte 8)))
               (table (copy-seq *wal-crc32-table*))
               (constants (copy-seq *native-ieee-crc32-constants*)))
          (dotimes (index (length bytes))
            (setf (aref bytes index) (mod (+ (* 73 index) (floor index 17)) 256)))
          (let ((before (copy-seq bytes)))
            (dolist (seed '(0 #xffffffff #x12345678 #x80000001))
              (dolist (size '(0 1 15 16 17 31 32 33 63 64 65 127 128 129 255 256 257 8191 8192))
                (dolist (start '(0 1 31))
                  (rove:ok (= (unit-crc32-bit-reference-seeded bytes start (+ start size) seed)
                              (%wal-crc32-ieee-octets bytes start (+ start size) table constants seed))))))
            (rove:ok (equalp bytes before)))
          (rove:ok (equalp table *wal-crc32-table*))
          (rove:ok (equalp constants *native-ieee-crc32-constants*)))
        (rove:skip "CPU has no PCLMULQDQ; scalar fallback remains covered.")))

#+x86-64
(rove:deftest native-crc32-unchecked-loads-cover-all-short-lengths-and-alignments
    (if *native-ieee-crc32-available-p*
        (let ((mismatch nil)
              (checks 0))
          (block check
            (loop for size from 0 to 257
                  do (dotimes (start 32)
                       ;; End the array at END so loads at the slice boundary have
                       ;; no extra array elements available beyond it.
                       (let* ((end (+ start size))
                              (bytes (make-array end :element-type '(unsigned-byte 8))))
                         (dotimes (index end)
                           (setf (aref bytes index)
                                 (mod (+ 17 (* 73 index) (floor index 17)) 256)))
                         (dolist (seed '(0 #xffffffff #x12345678 #x80000001))
                           (unless (= (unit-crc32-bit-reference-seeded bytes start end seed)
                                      (%wal-crc32-ieee-octets
                                       bytes start end *wal-crc32-table*
                                       *native-ieee-crc32-constants* seed))
                             (setf mismatch (list size start seed))
                             (return-from check))
                           (incf checks))))))
          (rove:ok (null mismatch) (format nil "First mismatched size/offset/seed: ~s" mismatch))
          (rove:ok (= checks (* 258 32 4))))
        (rove:skip "CPU has no PCLMULQDQ; scalar fallback remains covered.")))

#+x86-64
(rove:deftest native-crc32-noncanonical-tables-retain-their-byte-recurrence
    (let ((bytes (make-array 2048 :element-type '(unsigned-byte 8) :initial-element 173))
          (*wal-crc32-table* (copy-seq *wal-crc32-table*)))
      (dotimes (slot 256)
        (setf (aref bytes 0) (logxor #xff slot))
        (setf (aref *wal-crc32-table* slot) (logxor 1 (aref *wal-crc32-table* slot)))
        (rove:ok (= (unit-crc32-generic-reference bytes 0 (length bytes))
                    (wal-crc32 bytes)))
        (setf (aref *wal-crc32-table* slot) (logxor 1 (aref *wal-crc32-table* slot))))))

#+x86-64
(cl-spec:defproperty crc32-native-and-scalar-paths-agree
    ((length (range integer 0 8192)) (salt (range integer 0 255)))
  (:about wal-crc32)
  (:trials (:normal 100))
  (let* ((start (mod salt 32))
         (bytes (make-array (+ start length) :element-type '(unsigned-byte 8))))
    (dotimes (index (length bytes))
      (setf (aref bytes index) (mod (+ salt (* 73 index)) 256)))
    (let ((before (copy-seq bytes))
          (expected (unit-crc32-bit-reference bytes start (length bytes)))
          (native (wal-crc32 bytes start)))
      (and (= expected native
              (let ((*native-ieee-crc32-available-p* nil)) (wal-crc32 bytes start)))
           (equalp bytes before)))))

#+x86-64
(rove:deftest native-crc32-generated-native-and-forced-scalar-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'crc32-native-and-scalar-paths-agree :seed 3401))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

#+x86-64
(rove:deftest native-crc32-feature-refresh-uses-cpuid-and-keeps-one-init-hook
    (let* ((actual (logbitp 1 (nth-value 2 (sb-vm::%cpu-identification 1 0))))
           (*native-ieee-crc32-available-p* (not actual)))
      (refresh-native-ieee-crc32-support)
      (rove:ok (eq actual *native-ieee-crc32-available-p*))
      (refresh-native-ieee-crc32-support)
      (rove:ok (eq actual *native-ieee-crc32-available-p*))
      (rove:ok (= 1 (count 'refresh-native-ieee-crc32-support sb-ext:*init-hooks*)))))
