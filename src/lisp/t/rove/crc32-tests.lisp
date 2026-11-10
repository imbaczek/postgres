(in-package #:pg)

(defun unit-crc32-bit-reference (bytes start end)
  (let ((crc #xffffffff))
    (loop for index from start below end
          do (setf crc (logxor crc (aref bytes index)))
             (dotimes (bit 8)
               (setf crc (if (oddp crc)
                             (logxor (ash crc -1) #xedb88320)
                             (ash crc -1)))))
    (logxor crc #xffffffff)))

(defun unit-crc32-generic-reference (bytes start end)
  (let ((crc #xffffffff))
    (loop for index from start below end
          for slot = (logand (logxor crc (aref bytes index)) #xff)
          do (setf crc (logxor (ash crc -8) (aref *wal-crc32-table* slot))))
    (logxor crc #xffffffff)))

(cl-spec:defgenerator unit-crc32-arguments ()
  (let* ((length (random 1025))
         (octets (make-array length :element-type '(unsigned-byte 8)))
         (left (random (1+ length)))
         (right (random (1+ length))))
    (dotimes (index length) (setf (aref octets index) (random 256)))
    (list (case (random 3)
            (0 (coerce (coerce octets 'list) 'vector))
            (1 (make-array length :element-type '(unsigned-byte 8)
                                  :initial-contents octets :adjustable t :fill-pointer length))
            (t octets))
          (min left right) (max left right))))

(cl-spec:defspec-function wal-crc32
    (:args (bytes (type vector)) (start (type integer)) (end (type integer)))
  (:args-generator unit-crc32-arguments)
  (:pre (and (<= 0 start end (length bytes))
             (every (lambda (octet) (typep octet '(unsigned-byte 8))) bytes)))
  (:returns (type (unsigned-byte 32)))
  (:post (= result (unit-crc32-bit-reference bytes start end))))

(cl-spec:defproperty crc32-preserves-slices-and-generic-vectors
    ((length (range integer 0 512)) (salt (range integer 0 255)))
  (:about wal-crc32)
  (:trials (:normal 100))
  (let ((bytes (make-array length :element-type '(unsigned-byte 8))))
    (dotimes (index length) (setf (aref bytes index) (mod (+ salt (* 73 index)) 256)))
    (let ((start (floor length 3)) (end (- length (floor length 4))))
      (= (unit-crc32-bit-reference bytes start end)
         (wal-crc32 bytes start end)
         (wal-crc32 (coerce (coerce bytes 'list) 'vector) start end)))))

(rove:deftest crc32-matches-ieee-and-empty-vectors
    (rove:ok (= #xcbf43926 (wal-crc32 (string->bytes "123456789"))))
  (rove:ok (zerop (wal-crc32 (make-array 0 :element-type '(unsigned-byte 8)))))
  (rove:ok (= #xcbf43926 (wal-crc32 (string->bytes "_123456789_") 1 10)))
  (progv '(*wal-crc32-table*) nil
    (rove:ok (zerop (wal-crc32 (make-array 0 :element-type '(unsigned-byte 8)))))))

(rove:deftest crc32-preserves-generic-ranges-and-table-bindings
    (flet ((outcome (function bytes start end)
             (handler-case (list :value (funcall function bytes start end))
               (error (condition) (list :error (type-of condition))))))
      (dolist (table (list *wal-crc32-table*
                           (make-array 256 :initial-contents (coerce *wal-crc32-table* 'list))
                           (make-array 1 :initial-element 0)))
        (let ((*wal-crc32-table* table))
          (dolist (bytes (list (make-array 4 :element-type '(unsigned-byte 8)
                                             :initial-contents '(0 1 128 255))
                               (vector 0 1 128 255) (vector 256 -1 :bad)
                               (make-array 4 :element-type '(unsigned-byte 8)
                                             :initial-contents '(0 1 128 255)
                                             :adjustable t :fill-pointer 3)
                               (make-array 2 :element-type '(unsigned-byte 8)
                                             :displaced-to (make-array 4 :element-type '(unsigned-byte 8)
                                                                         :initial-contents '(0 1 128 255))
                                             :displaced-index-offset 1)
                               "" "abc"))
            (dolist (range '((0 0) (0 1) (1 2) (2 1) (-1 0) (0 -1)
                             (0 100) (1/2 0) (1/2 2) (0 1/2) ("bad" 0)))
              (destructuring-bind (start end) range
                (rove:ok (equal (outcome #'unit-crc32-generic-reference bytes start end)
                                (outcome #'wal-crc32 bytes start end))))))))))

(rove:deftest crc32-generated-function-and-slice-contracts
    (dolist (data (list (cl-spec:result-data
                         (cl-spec:check-function 'wal-crc32 :trials 100 :seed 241))
                        (cl-spec:result-data
                         (cl-spec:run-property 'crc32-preserves-slices-and-generic-vectors
                                               :seed 242))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))
