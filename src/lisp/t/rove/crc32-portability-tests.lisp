(in-package #:pg)

(rove:deftest crc32-arm-auxv-probe-fails-closed
    (uiop:with-temporary-file (:pathname path)
      (flet ((write-pairs (pairs)
               (with-open-file (output path :direction :output :if-exists :supersede
                                            :element-type '(unsigned-byte 8))
                 (dolist (pair pairs)
                   (dolist (word (list (car pair) (cdr pair)))
                     (dotimes (index 8)
                       (write-byte (ldb (byte 8 (* index 8)) word) output)))))))
        (write-pairs '((6 . 4096) (16 . #x90) (0 . 0)))
        (rove:ok (= #x90 (native-arm-read-auxv-hwcap path)))
        (write-pairs '((16 . 0) (0 . 0)))
        (rove:ok (eql 0 (native-arm-read-auxv-hwcap path)))
        (write-pairs '((6 . 4096) (0 . 0)))
        (rove:ok (null (native-arm-read-auxv-hwcap path)))
        (write-pairs (loop repeat 130 collect '(6 . 4096)))
        (rove:ok (null (native-arm-read-auxv-hwcap path)))
        (with-open-file (output path :direction :output :if-exists :supersede
                                     :element-type '(unsigned-byte 8))
          (write-byte 16 output))
        (rove:ok (null (native-arm-read-auxv-hwcap path)))
        (delete-file path)
        (rove:ok (null (native-arm-read-auxv-hwcap path))))))

(rove:deftest crc32-disabled-features-never-enter-native-code
    (let* ((bytes (make-array 2048 :element-type '(unsigned-byte 8) :initial-element 173))
           (*native-arm-crc32-available-p* nil)
           #+x86-64 (*native-crc32c-sse42-available-p* nil)
           #+x86-64 (*native-crc32c-pclmul-available-p* nil)
           (natives #+x86-64 '(%wal-crc32c-sse42-octets %wal-crc32c-pclmul-octets)
                    #+arm64 '(%wal-crc32c-arm-octets)
                    #-(or x86-64 arm64) nil)
           (originals (mapcar #'symbol-function natives)))
      (unwind-protect
           (progn
             (dolist (native natives)
               (setf (symbol-function native)
                     (lambda (&rest arguments)
                       (declare (ignore arguments))
                       (error "Disabled native CRC was called."))))
             (dolist (size '(0 1 7 8 15 16 31 32 63 64 127 128 129 256 513 1024))
               (rove:ok (= (unit-crc32-bit-reference bytes 3 (+ 3 size))
                           (wal-crc32 bytes 3 (+ 3 size))))))
        (mapc (lambda (native original) (setf (symbol-function native) original))
              natives originals))))

(rove:deftest crc32-arm-feature-refresh-keeps-one-init-hook
    (rove:ok (= 1 (count 'refresh-native-arm-crc32-support sb-ext:*init-hooks*)))
  (refresh-native-arm-crc32-support)
  (refresh-native-arm-crc32-support)
  (rove:ok (= 1 (count 'refresh-native-arm-crc32-support sb-ext:*init-hooks*)))
  #-(and arm64 little-endian)
  (progn
    (rove:ok (null *native-arm-crc32-available-p*))
    (rove:ok (null *native-arm-pmull-available-p*))))
