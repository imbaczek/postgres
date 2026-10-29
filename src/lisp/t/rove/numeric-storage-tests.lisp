;;;; Numeric packed-format regressions against numeric.c layout.
(in-package #:pg)

(rove:deftest test-packed-numeric-known-values
    (dolist (case '(("0" (24 0 0 0 0 128))
                    ("1.23" (40 0 0 0 0 129 1 0 252 8))
                    ("-0.001" (32 0 0 0 255 161 10 0))
                    ("10000" (32 0 0 0 1 128 1 0))
                    ("0.00" (24 0 0 0 0 129))
                    ("1e256" (40 0 0 0 0 0 64 0 1 0))
                    ("0.0000000000000000000000000000000000000000000000000000000000000001"
                     (40 0 0 0 64 0 240 255 1 0))))
      (let* ((value (sql-numeric-input (first case)))
             (expected (make-array (length (second case))
                                   :element-type '(unsigned-byte 8)
                                   :initial-contents (second case)))
             (encoded (sql-numeric-to-packed-octets value)))
        (rove:ok (equalp encoded expected))
        (rove:ok (string= (sql-numeric-output value)
                          (sql-numeric-output (sql-numeric-from-packed-octets expected)))))))

(rove:deftest test-packed-numeric-long-and-malformed
    (dolist (text '("123456789012345678901234567890.00000000000000000001"
                    "0.0000000000000000000000000000000000000000000000000000000000000001"
                    "-999999999999999999999999999999999999999999999999999999999999999999999"))
      (let* ((value (sql-numeric-input text))
             (encoded (sql-numeric-to-packed-octets value)))
        (rove:ok (string= (sql-numeric-output value)
                          (sql-numeric-output (sql-numeric-from-packed-octets encoded))))))
  (let* ((bad (sql-numeric-to-packed-octets (sql-numeric-input "1"))))
    (page-set-u16 bad 6 10000)
    (rove:ok (handler-case (progn (sql-numeric-from-packed-octets bad) nil)
               (error () t)))))

(rove:deftest test-packed-numeric-invalid-framing
    (let ((one (sql-numeric-to-packed-octets (sql-numeric-input "1"))))
      (dolist (bad (list (subseq one 0 7)
                         (let ((copy (copy-seq one)))
                           (page-set-u32 copy 0 24) copy)
                         (let ((copy (copy-seq one)))
                           (page-set-u16 copy 4 #x807f) copy)
                         (let ((copy (copy-seq one)))
                           (page-set-u16 copy 4 #xc000) copy)))
        (rove:ok (handler-case (progn (sql-numeric-from-packed-octets bad) nil)
                   (error () t))))))
