(in-package #:pg)

#+arm64
(defun unit-arm-crc32-reference (bytes start end seed polynomial)
  (let ((crc seed))
    (loop for index from start below end
          do (setf crc (logxor crc (aref bytes index)))
             (dotimes (bit 8)
               (setf crc (logxor (ash crc -1) (if (oddp crc) polynomial 0)))))
    (logxor crc #xffffffff)))

#+arm64
(setf (gethash '%wal-crc32-ieee-arm-octets *unit-hot-probes*)
      (let ((bytes (make-array 1100 :element-type '(unsigned-byte 8) :initial-element 173)))
        (loop for seed in '(0 #xffffffff #x80000001)
              append (loop for size in (if *native-arm-crc32-available-p*
                                           '(0 1 2 3 4 7 8 9 15 16 17 127 128 129 1024)
                                           '(0))
                           collect (list bytes 3 (+ 3 size) nil nil seed))))
      (gethash '%wal-crc32c-arm-octets *unit-hot-probes*)
      (gethash '%wal-crc32-ieee-arm-octets *unit-hot-probes*))

#+arm64
(rove:deftest native-arm-crc32-ieee-and-castagnoli-match-bit-reference
    (if *native-arm-crc32-available-p*
        (let ((mismatch nil)
              (checks 0))
          (block check
            (loop for size from 0 to 257
                  do (dolist (start '(0 1 2 3 4 5 6 7 15 16 31))
                       (let* ((end (+ start size))
                              (bytes (make-array end :element-type '(unsigned-byte 8))))
                         (dotimes (index end)
                           (setf (aref bytes index)
                                 (mod (+ 17 (* 73 index) (floor index 17)) 256)))
                         (dolist (seed '(0 #xffffffff #x12345678 #x80000001))
                           (dolist (case (list (list #'%wal-crc32-ieee-arm-octets #xedb88320)
                                               (list #'%wal-crc32c-arm-octets #x82f63b78)))
                             (unless (= (unit-arm-crc32-reference bytes start end seed (second case))
                                        (funcall (first case) bytes start end nil nil seed))
                               (setf mismatch (list size start seed (second case)))
                               (return-from check))
                             (incf checks)))))))
          (rove:ok (null mismatch) (format nil "ARM CRC mismatch: ~s" mismatch))
          (rove:ok (= checks (* 258 11 4 2))))
        (rove:skip "CPU has no CRC32 extension; portable fallback remains covered.")))
