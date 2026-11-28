(in-package #:pg)

(defun unit-crc32-bit-reference (bytes start end)
  (let ((crc #xffffffff))
    (loop for index from start below end
          do (setf crc (logxor crc (aref bytes index)))
             (dotimes (bit 8)
               (setf crc (if (oddp crc)
                             (logxor (ash crc -1) #x82f63b78)
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

(rove:deftest crc32-matches-castagnoli-and-empty-vectors
    (rove:ok (= #xe3069283 (wal-crc32 (string->bytes "123456789"))))
  (rove:ok (zerop (wal-crc32 (make-array 0 :element-type '(unsigned-byte 8)))))
  (rove:ok (= #xe3069283 (wal-crc32 (string->bytes "_123456789_") 1 10)))
  (progv '(*wal-crc32-table*) nil
    (rove:ok (zerop (wal-crc32 (make-array 0 :element-type '(unsigned-byte 8)))))))

(rove:deftest crc32-can-continue-from-a-prefix
    (let* ((bytes (make-array 512 :element-type '(unsigned-byte 8)
                                  :initial-contents (loop for i below 512 collect (mod (* i 19) 256))))
           (displaced (make-array 512 :element-type '(unsigned-byte 8) :displaced-to bytes))
           (expected (wal-crc32 bytes)))
      (dolist (input (list bytes displaced))
        (dolist (split '(0 31 64 129 480 512))
          (let ((raw (logxor #xffffffff (wal-crc32 input 0 split))))
            (rove:ok (= expected (wal-crc32 input split (length input) raw))))))))

(rove:deftest retained-wal-envelope-can-borrow-stable-payload
    (let* ((magic #(80 71 82 69 88 84 48 49))
           (payload (make-array 4 :element-type '(unsigned-byte 8)
                                  :initial-contents '(4 1 0 7)))
           (image (physical-retained-envelope-encode magic '(:test 1) payload)))
      (multiple-value-bind (metadata owned) (physical-retained-envelope-decode magic image)
        (multiple-value-bind (borrowed-metadata borrowed)
            (physical-retained-envelope-decode magic image :borrow-image-p t)
          (rove:ok (equal metadata borrowed-metadata))
          (rove:ok (and (equalp owned payload) (equalp borrowed payload)))
          (rove:ok (null (array-displacement owned)))
          (rove:ok (eq image (array-displacement borrowed)))))))

(rove:deftest wal-record-decoder-can-borrow-stable-payload
    (let* ((payload (make-array 3 :element-type '(unsigned-byte 8)
                                  :initial-contents '(11 0 255)))
           (bytes (wal-encode-record +wal-record-pages+ 7 42 payload))
           (owned (wal-record-payload (wal-decode-record bytes 42)))
           (borrowed (wal-record-payload
                      (wal-decode-record bytes 42 :borrow-payload-p t))))
      (rove:ok (equalp owned payload))
      (rove:ok (equalp borrowed payload))
      (rove:ok (null (array-displacement owned)))
      (multiple-value-bind (underlying offset) (array-displacement borrowed)
        (rove:ok (eq underlying bytes))
        (rove:ok (= offset +wal-header-size+)))))

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
