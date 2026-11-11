(in-package #:pg)

#+x86-64
(defun unit-native-crc32c-reference (bytes start end seed)
  (let ((crc seed))
    (loop for index from start below end
          do (setf crc (logxor crc (aref bytes index)))
             (dotimes (bit 8)
               (setf crc (logxor (ash crc -1) (if (oddp crc) #x82f63b78 0)))))
    (logxor crc #xffffffff)))

#+x86-64
(defun unit-native-crc32c-functions ()
  (append (when *native-crc32c-sse42-available-p* (list #'%wal-crc32c-sse42-octets))
          (when *native-crc32c-pclmul-available-p* (list #'%wal-crc32c-pclmul-octets))))

#+x86-64
(setf (gethash '%wal-crc32c-sse42-octets *unit-hot-probes*)
      (let ((bytes (make-array 1100 :element-type '(unsigned-byte 8) :initial-element 173)))
        (loop for seed in '(0 #xffffffff #x80000001)
              append (loop for size in (if *native-crc32c-sse42-available-p*
                                           '(0 1 3 4 7 8 9 15 16 63 64 65 127 128 129 1024)
                                           '(0))
                           collect (list bytes 3 (+ 3 size) nil nil seed))))
      (gethash '%wal-crc32c-pclmul-octets *unit-hot-probes*)
      (let ((bytes (make-array 1100 :element-type '(unsigned-byte 8) :initial-element 173)))
        (loop for seed in '(0 #xffffffff #x80000001)
              append (loop for size in (if *native-crc32c-pclmul-available-p*
                                           '(0 1 3 4 7 8 9 15 16 63 64 65 127 128 129 255 256 257 1024)
                                           '(0))
                           collect (list bytes 3 (+ 3 size) nil *native-crc32c-constants* seed)))))

#+x86-64
(rove:deftest native-crc32c-exact-ended-slices-and-seeds
    (let ((functions (unit-native-crc32c-functions))
          (constants (copy-seq *native-crc32c-constants*))
          (checks 0))
      (if functions
          (progn
            (dolist (size (append (loop for size from 0 to 257 collect size)
                                  '(511 512 513 8191 8192 8193)))
              (dolist (start '(0 1 3 7 8 15 16 31 63))
                (let* ((end (+ start size))
                       (bytes (make-array end :element-type '(unsigned-byte 8))))
                  (dotimes (index end)
                    (setf (aref bytes index) (mod (+ 17 (* 73 index) (floor index 17)) 256)))
                  (let ((before (copy-seq bytes)))
                    (dolist (seed '(0 #xffffffff #x12345678 #x80000001))
                      (let ((expected (unit-native-crc32c-reference bytes start end seed)))
                        (dolist (fn functions)
                          (assert (= expected (funcall fn bytes start end nil constants seed)))
                          (incf checks))))
                    (assert (equalp bytes before))))))
            (rove:ok (= checks (* 264 9 4 (length functions))))
            (rove:ok (equalp constants *native-crc32c-constants*)))
          (rove:skip "CPU has no SSE4.2; portable CRC32C remains covered."))))

#+x86-64
(rove:deftest native-crc32c-continuation-preserves-raw-state
    (let ((functions (unit-native-crc32c-functions))
          (bytes (make-array 1100 :element-type '(unsigned-byte 8)))
          (checks 0))
      (dotimes (index (length bytes))
        (setf (aref bytes index) (mod (+ 31 (* index 109)) 256)))
      (if functions
          (progn
            (dolist (size '(0 1 7 8 15 16 31 32 63 64 65 127 128 129 255 256 257 1000))
              (dolist (seed '(0 #xffffffff #x12345678))
                (let ((expected (unit-native-crc32c-reference bytes 3 (+ 3 size) seed)))
                  (loop for split from 0 to size
                        do (dolist (fn functions)
                             (let* ((first (funcall fn bytes 3 (+ 3 split)
                                                    nil *native-crc32c-constants* seed))
                                    (continued (funcall fn bytes (+ 3 split) (+ 3 size)
                                                        nil *native-crc32c-constants*
                                                        (logxor first #xffffffff))))
                               (assert (= expected continued))
                               (incf checks)))))))
            (rove:ok (plusp checks)))
          (rove:skip "CPU has no SSE4.2; portable CRC32C remains covered."))))

#+x86-64
(rove:deftest native-crc32c-feature-refresh-keeps-one-init-hook
    (let* ((ecx (nth-value 2 (sb-vm::%cpu-identification 1 0)))
           (*native-crc32c-sse42-available-p* nil)
           (*native-crc32c-pclmul-available-p* nil))
      (refresh-native-crc32c-support)
      (rove:ok (eq *native-crc32c-sse42-available-p* (logbitp 20 ecx)))
      (rove:ok (eq *native-crc32c-pclmul-available-p*
                   (and (logbitp 20 ecx) (logbitp 1 ecx))))
      (refresh-native-crc32c-support)
      (rove:ok (= 1 (count 'refresh-native-crc32c-support sb-ext:*init-hooks*)))))
