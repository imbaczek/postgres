(in-package #:pg)

(defun unit-fnv64-reference (digest bytes)
  (loop for octet across bytes
        do (setf digest (logand #xffffffffffffffff
                                (* (logxor digest octet) #x100000001b3))))
  digest)

(defun unit-fnv64-outcome (function digest bytes)
  (handler-case (list :value (funcall function digest bytes))
    (error (condition) (list :error (type-of condition)))))

(cl-spec:defgenerator unit-fnv64-arguments ()
  (let* ((length (random 1025))
         (bytes (make-array length :element-type '(unsigned-byte 8)))
         (seed (case (random 5)
                 (0 0) (1 #xffffffffffffffff) (2 #x8000000000000000)
                 (t (random (ash 1 64))))))
    (dotimes (index length) (setf (aref bytes index) (random 256)))
    (list seed (case (random 3)
                 (0 (coerce (coerce bytes 'list) 'vector))
                 (1 (make-array length :element-type '(unsigned-byte 8)
                                       :initial-contents bytes :adjustable t :fill-pointer length))
                 (t bytes)))))

(cl-spec:defspec-function physical-store-update-digest
    (:args (digest (type (unsigned-byte 64))) (bytes (type vector)))
  (:args-generator unit-fnv64-arguments)
  (:pre (every (lambda (octet) (typep octet '(unsigned-byte 8))) bytes))
  (:returns (type (unsigned-byte 64)))
  (:post (and (= result (unit-fnv64-reference digest bytes))
              (or (plusp (length bytes)) (eq result digest)))))

(cl-spec:defproperty fnv64-preserves-prefix-composition
    ((length (range integer 0 1024)) (salt (range integer 0 255)))
  (:about physical-store-update-digest)
  (:trials (:normal 100))
  (let* ((bytes (make-array length :element-type '(unsigned-byte 8)))
         (split (floor length 2))
         (seed #xcbf29ce484222325))
    (dotimes (index length) (setf (aref bytes index) (mod (+ salt (* index 73)) 256)))
    (= (unit-fnv64-reference seed bytes)
       (physical-store-update-digest seed bytes)
       (physical-store-update-digest
        (physical-store-update-digest seed (subseq bytes 0 split))
        (subseq bytes split)))))

(defun unit-fnv64-span-reference (digest bytes start end)
  (unit-fnv64-reference digest (subseq bytes start end)))

(defun unit-fnv64-span-outcome (function digest bytes start end)
  (handler-case (list :value (funcall function digest bytes start end))
    (error (condition) (list :error (type-of condition)))))

(cl-spec:defgenerator unit-fnv64-span-arguments ()
  (destructuring-bind (seed bytes) (funcall (cl-spec:custom-generator-function
                                             (cl-spec:find-generator 'unit-fnv64-arguments)))
    (let* ((length (length bytes))
           (start (random (1+ length)))
           (end (+ start (random (1+ (- length start))))))
      (list seed bytes start end))))

(cl-spec:defspec-function physical-store-update-digest-span
    (:args (digest (type (unsigned-byte 64))) (bytes (type vector))
           (start (type fixnum)) (end (type fixnum)))
  (:args-generator unit-fnv64-span-arguments)
  (:pre (and (<= 0 start end (length bytes))
             (every (lambda (octet) (typep octet '(unsigned-byte 8))) bytes)))
  (:returns (type (unsigned-byte 64)))
  (:post (and (= result (unit-fnv64-span-reference digest bytes start end))
              (or (< start end) (eq result digest)))))

(cl-spec:defproperty fnv64-span-preserves-prefix-composition
    ((length (range integer 0 1024)) (salt (range integer 0 255))
                                     (left (range integer 0 128)) (right (range integer 0 128)))
  (:about physical-store-update-digest-span)
  (:trials (:normal 100))
  (let* ((bytes (make-array (+ left length right) :element-type '(unsigned-byte 8)))
         (split (+ left (floor length 2)))
         (end (+ left length))
         (seed #xcbf29ce484222325))
    (dotimes (index (length bytes))
      (setf (aref bytes index) (mod (+ salt (* index 73)) 256)))
    (= (unit-fnv64-span-reference seed bytes left end)
       (physical-store-update-digest-span seed bytes left end)
       (physical-store-update-digest-span
        (physical-store-update-digest-span seed bytes left split)
        bytes split end))))

(rove:deftest fnv64-span-preserves-slice-values-and-errors
    (let ((base (make-array 4 :element-type '(unsigned-byte 8)
                              :initial-contents '(0 1 128 255))))
      (dolist (seed (list -1 (ash 1 100) 1/2 :opaque #xcbf29ce484222325))
        (dolist (bytes (list (make-array 0 :element-type '(unsigned-byte 8))
                             base (vector 0 1 128 255) (vector :outside 0 1 255 :outside)
                             (make-array 4 :element-type '(unsigned-byte 8)
                                           :initial-contents base :adjustable t :fill-pointer 3)
                             (make-array 2 :element-type '(unsigned-byte 8)
                                           :displaced-to base :displaced-index-offset 1)
                             "" "abc" '(0 1)))
          (dolist (span '((0 0) (0 nil) (0 1) (1 1) (1 3) (0 4)
                          (-1 0) (3 1) (0 100) (:bad 1) (0 :bad)))
            (destructuring-bind (start end) span
              (rove:ok
               (equal (unit-fnv64-span-outcome #'unit-fnv64-span-reference seed bytes start end)
                      (unit-fnv64-span-outcome #'physical-store-update-digest-span
                                               seed bytes start end)))))))))

(rove:deftest fnv64-span-excludes-unselected-data-and-preserves-empty-identity
    (dolist (seed (list #xffffffffffffffff (list :opaque-seed)))
      (let ((bytes (make-array 6 :element-type '(unsigned-byte 8) :initial-element 255)))
        (rove:ok (eq seed (physical-store-update-digest-span seed bytes 3 3)))))
  (let ((bytes (vector :outside 0 1 128 255 :outside)))
    (rove:ok (= (unit-fnv64-reference #xcbf29ce484222325 #(0 1 128 255))
                (physical-store-update-digest-span #xcbf29ce484222325 bytes 1 5)))))

(rove:deftest fnv64-span-generated-function-and-prefix-contracts
    (dolist (data (list (cl-spec:result-data
                         (cl-spec:check-function 'physical-store-update-digest-span
                                                 :trials 100 :seed 643))
                        (cl-spec:result-data
                         (cl-spec:run-property 'fnv64-span-preserves-prefix-composition :seed 644))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest fnv64-matches-historical-vectors-and-empty-identity
    (rove:ok (= #xa430d84680aabd0b
                (physical-store-update-digest #xcbf29ce484222325 (string->bytes "hello"))))
  (rove:ok (= #x85944171f73967e8
              (physical-store-update-digest #xcbf29ce484222325 (string->bytes "foobar"))))
  (dolist (seed (list #xffffffffffffffff (list :opaque-seed)))
    (dolist (bytes (list (make-array 0 :element-type '(unsigned-byte 8)) (vector) ""))
      (rove:ok (eq seed (physical-store-update-digest seed bytes)))))
  (dolist (seed '(0 1 #x1fffffffffffffff #x2000000000000000 #x8000000000000000
                  #xfffffffffffffffe #xffffffffffffffff))
    (dolist (octet '(0 255))
      (let ((bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element octet)))
        (rove:ok (= (unit-fnv64-reference seed bytes)
                    (physical-store-update-digest seed bytes))))))
  (dolist (length '(8191 8192 8193 65535 65536 65537))
    (let ((bytes (make-array length :element-type '(unsigned-byte 8) :initial-element 255)))
      (rove:ok (= (unit-fnv64-reference #xffffffffffffffff bytes)
                  (physical-store-update-digest #xffffffffffffffff bytes))))))

(rove:deftest fnv64-preserves-generic-input-and-error-behavior
    (let ((base (make-array 4 :element-type '(unsigned-byte 8) :initial-contents '(0 1 128 255))))
      (dolist (seed (list -1 (ash 1 100) 1/2 :opaque #xcbf29ce484222325))
        (dolist (bytes (list (make-array 0 :element-type '(unsigned-byte 8))
                             base (vector 0 1 128 255) (vector 0 256 -1)
                             (make-array 4 :element-type '(unsigned-byte 8)
                                           :initial-contents base :adjustable t :fill-pointer 3)
                             (make-array 2 :element-type '(unsigned-byte 8)
                                           :displaced-to base :displaced-index-offset 1)
                             "" "abc" '(0 1)))
          (rove:ok (equal (unit-fnv64-outcome #'unit-fnv64-reference seed bytes)
                          (unit-fnv64-outcome #'physical-store-update-digest seed bytes)))))))

(rove:deftest fnv64-generated-function-and-prefix-contracts
    (dolist (data (list (cl-spec:result-data
                         (cl-spec:check-function 'physical-store-update-digest :trials 100 :seed 243))
                        (cl-spec:result-data
                         (cl-spec:run-property 'fnv64-preserves-prefix-composition :seed 244))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))
