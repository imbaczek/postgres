(in-package #:pg)

(cl-spec:defproperty ascii-span-decoding-matches-sbcl
    ((size (range integer 0 513)) (offset (range integer 0 7))
                                  (seed (range integer 0 127)))
  (:about bytes->string)
  (:trials (:normal 300))
  (let* ((bytes (make-array (+ offset size 2) :element-type '(unsigned-byte 8)
                                              :initial-element 255)))
    (dotimes (index size)
      (setf (aref bytes (+ offset index)) (mod (+ seed index) 128)))
    (let ((expected (sb-ext:octets-to-string bytes :start offset :end (+ offset size)
                                                   :external-format :utf-8))
          (actual (bytes->string bytes :start offset :end (+ offset size))))
      (and (string= expected actual)
           (equal (array-element-type expected) (array-element-type actual))))))

(rove:deftest ascii-span-generated-decoding-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'ascii-span-decoding-matches-sbcl
                                       :seed 613 :profile :normal))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest ascii-span-all-codes-own-general-character-output
    (let ((bytes (make-array 130 :element-type '(unsigned-byte 8) :initial-element 255)))
      (dotimes (index 128) (setf (aref bytes (1+ index)) index))
      (let ((result (bytes->string bytes :start 1 :end 129))
            (other (bytes->string bytes :start 1 :end 129)))
        (rove:ok (equal '(simple-array character (128)) (type-of result)))
        (dotimes (index 128)
          (rove:ok (= index (char-code (char result index)))))
        (fill bytes 65)
        (setf (char result 0) #\X)
        (rove:ok (zerop (char-code (char other 0))))
        (rove:ok (= 127 (char-code (char result 127))))))
  (let ((bytes (make-array 8 :element-type '(unsigned-byte 8) :initial-element 65)))
    (rove:ok (string= "AAAAAAA" (bytes->string bytes :start 1 :end nil)))
    (rove:ok (string= "" (bytes->string bytes :start 8 :end nil)))))

(rove:deftest ascii-span-detects-high-bit-at-word-and-tail-boundaries
    (let ((bytes (make-array 80 :element-type '(unsigned-byte 8) :initial-element 65)))
      (dotimes (start 8)
        (dolist (size '(0 7 8 31 32 33 39 40 63 64 65))
          (let ((end (+ start size)))
            (setf (aref bytes end) 128)
            (rove:ok (%octet-span-ascii-p bytes start end)
                     "An octet beyond the requested span is ignored")
            (setf (aref bytes end) 65)
            (loop for position from start below end
                  do (setf (aref bytes position) 128)
                     (rove:ok (not (%octet-span-ascii-p bytes start end)))
                     (setf (aref bytes position) 65)))))))

(rove:deftest compact-ascii-span-keeps-utf8-fallback
    (let* ((bytes (make-array 131 :element-type '(unsigned-byte 8)
                                  :initial-element 255)))
      (dotimes (index 128) (setf (aref bytes (1+ index)) index))
      (let ((compact (bytes->string bytes :start 1 :end 129 :compact-ascii-p t)))
        (rove:ok (typep compact 'simple-base-string))
        (rove:ok (string= compact (bytes->string bytes :start 1 :end 129)))
        (fill bytes 65)
        (rove:ok (= 127 (char-code (char compact 127)))))
      (let* ((text "Zażółć")
             (utf8 (string->bytes text)))
        (rove:ok (string= text (bytes->string utf8 :compact-ascii-p t))))))

(rove:deftest ascii-span-fallback-preserves-sbcl-outcomes
    (dolist (bytes (list (make-array 4 :element-type '(unsigned-byte 8)
                                       :initial-contents '(65 128 66 255))
                         (make-array 4 :element-type '(unsigned-byte 8)
                                       :initial-element 65 :adjustable t :fill-pointer 2)
                         #(65 66 67) '(65 66 67)))
      (dolist (bounds '((0 0) (0 nil) (0 1) (0 3) (1 3) (-1 1)
                        (0 -1) (2 1) (:bad 1) (0 :bad)))
        (destructuring-bind (start end) bounds
          (flet ((outcome (function)
                   (handler-case (list :value (funcall function bytes :start start :end end))
                     (error (condition) (list :error (type-of condition))))))
            (rove:ok
             (equal (outcome #'bytes->string)
                    (outcome (lambda (bytes &key start end)
                               (sb-ext:octets-to-string bytes :start start :end end
                                                              :external-format :utf-8))))))))))

(setf (gethash '%octet-span-ascii-p *unit-hot-probes*)
      (list (list (make-array 0 :element-type '(unsigned-byte 8)) 0 0)
            (list (make-array 129 :element-type '(unsigned-byte 8) :initial-element 127) 1 129)
            (list (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(0 128 127)) 0 3))
      (gethash '%octet-span-copy-ascii *unit-hot-probes*)
      (list (list (make-array 0 :element-type '(unsigned-byte 8)) 0 0 (make-string 0))
            (list (make-array 129 :element-type '(unsigned-byte 8) :initial-element 127)
                  1 129 (make-string 128))
            (list (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(0 65 127))
                  0 3 (make-string 3))
            (list (make-array 130 :element-type '(unsigned-byte 8) :initial-element 127)
                  1 129 (make-string 128 :element-type 'base-char))))
