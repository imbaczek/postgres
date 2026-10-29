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

(rove:deftest test-numeric-varlena-tuple
    (let ((columns (vector (make-pg-column :type :int2)
                           (make-pg-column :type :numeric)
                           (make-pg-column :type :int4))))
      (dolist (value (list (sql-numeric-input "123.4500")
                           (sql-numeric-input "0.0000001")
                           :numeric-nan :numeric-positive-infinity
                           :numeric-negative-infinity))
        (let* ((logical (vector 7 value 42))
               (tuple (form-heap-tuple logical columns 42 0 0 0 1))
               (decoded (deform-heap-tuple tuple columns)))
          (rove:ok (= (length tuple) (heap-tuple-layout logical columns)))
          (rove:ok (and (= (aref decoded 0) 7) (= (aref decoded 2) 42)))
          (rove:ok (equal (sql-numeric-output value)
                          (sql-numeric-output (aref decoded 1)))))))
  (dolist (value '(:numeric-nan :numeric-positive-infinity :numeric-negative-infinity))
    (rove:ok (eq value (sql-numeric-from-varlena-payload
                        (sql-numeric-to-varlena-payload value))))))

(rove:deftest test-numeric-toast-planning
    (let* ((columns (vector (make-pg-column :type :numeric)))
           (repeated (sql-numeric-input (make-string 8000 :initial-element #\1)))
           (digits (make-string 8000 :initial-element #\7))
           (state 1))
      (loop for i from 1 below (length digits)
            do (setf state (logand #xffffffff (+ (* state 1664525) 1013904223))
                     (char digits i) (digit-char (mod (ldb (byte 8 24) state) 10))))
      (let* ((random (sql-numeric-input digits))
             (inline (toast-compress-inline-values (vector repeated) columns))
             (large (toast-compress-inline-values (vector random) columns)))
        (rove:ok (heap-compressed-varlena-p (aref inline 0)))
        (rove:ok (< (heap-tuple-layout inline columns) +toast-tuple-target+))
        (rove:ok (> (heap-tuple-layout large columns) +toast-tuple-target+))
        (rove:ok (equal (toast-compressed-externalization-plan large columns) '(0))))))
