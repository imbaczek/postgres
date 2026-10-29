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
