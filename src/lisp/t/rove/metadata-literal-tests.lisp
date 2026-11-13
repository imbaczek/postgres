(in-package #:pg)

(defun unit-metadata-reference-text (value)
  (labels ((literal (value)
             (typecase value
               (string (map 'string #'identity value))
               (cons (mapcar #'literal value))
               (t value))))
    (with-standard-io-syntax (prin1-to-string (literal value)))))

(defun unit-metadata-string (size seed shape)
  (let ((text (make-string size :element-type (if (eq shape :base) 'base-char 'character))))
    (dotimes (index size)
      (setf (char text index)
            (code-char (if (and (not (eq shape :base)) (zerop (mod index 7)))
                           (+ 256 (mod seed 1024))
                           (mod (+ seed index) 128)))))
    (ecase shape
      ((:base :character) text)
      (:adjustable
       (let ((result (make-array (+ size 4) :element-type 'character :adjustable t
                                            :fill-pointer size :initial-element #\X)))
         (replace result text)
         result))
      (:displaced
       (let ((backing (concatenate 'string "xx" text "yy")))
         (make-array size :element-type 'character :displaced-to backing
                          :displaced-index-offset 2))))))

(cl-spec:defproperty metadata-literal-printing-keeps-canonical-text
    ((size (range integer 0 128)) (seed (range integer 0 10000))
                                  (shape (member :base :character :adjustable :displaced)))
  (:about physical-bootstrap-manifest-text)
  (:trials (:normal 400))
  (let* ((text (unit-metadata-string size seed shape))
         (value (list :version seed :flag (oddp seed) :empty nil
                      :keyword (if (oddp seed) :|MiXeD| :|two words|)
                      :files (loop for index below (mod size 9)
                                   collect (list text index (+ (ash 1 80) seed index))))))
    (string= (unit-metadata-reference-text value)
             (physical-bootstrap-manifest-text value))))

(rove:deftest metadata-literal-generated-canonical-text-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'metadata-literal-printing-keeps-canonical-text
                                       :seed 714 :profile :normal))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest metadata-literal-preserves-fallback-values-and-errors
    (dolist (value (list 3/7 1.25d0 #P"relative/file" #\A #(1 "base") #*1010
                         'car (make-hash-table) #'car *standard-output* (cons :bad :tail)))
      (dolist (input (list value (list :nested value)))
        (flet ((outcome (function)
                 (handler-case (list :value (funcall function input))
                   (error (condition) (list :error (type-of condition))))))
          (rove:ok (equal (outcome #'unit-metadata-reference-text)
                          (outcome #'physical-bootstrap-manifest-text)))))))

(rove:deftest metadata-literal-printing-preserves-input-and-output-ownership
    (dolist (shape '(:base :character :adjustable :displaced))
      (let* ((text (unit-metadata-string 128 0 shape))
             (original (copy-seq text))
             (value (list :text text :again text))
             (expected (unit-metadata-reference-text value))
             (result (physical-bootstrap-manifest-text value)))
        (rove:ok (string= original text))
        (rove:ok (string= expected result))
        (fill text #\Z)
        (rove:ok (string= expected result))))
  (let* ((value (list :reader "(#.(error) and literal text)" :key :|MiXeD|))
         (expected (unit-metadata-reference-text value)))
    (let ((*package* (find-package :keyword)) (*print-base* 16) (*print-case* :downcase)
                                              (*print-length* 1) (*print-level* 1) (*print-circle* t) (*print-escape* nil))
      (rove:ok (string= expected (physical-bootstrap-manifest-text value)))))
  (let ((deep (loop repeat 70 for value = (list :value) then (list value) finally (return value))))
    (rove:ok (string= (unit-metadata-reference-text deep)
                      (physical-bootstrap-manifest-text deep)))))

(rove:deftest metadata-literal-fast-guard-rejects-unsafe-shapes
    (dolist (value (list nil t :ok 0 (ash 1 100) "" (list :ok "text" (list 1 nil))))
      (rove:ok (%metadata-literal-tree-p value 0)))
  (dolist (value (list 'car #(1 2) 1/3 #\A (cons :bad :tail)))
    (rove:ok (not (%metadata-literal-tree-p value 0))))
  (let ((spine (list :ok)) (nested (list nil)))
    (setf (cdr spine) spine (car nested) nested)
    (rove:ok (not (%metadata-literal-tree-p spine 0)))
    (rove:ok (not (%metadata-literal-tree-p nested 0))))
  (let ((value :ok))
    (dotimes (level 63) (setf value (list value)))
    (rove:ok (%metadata-literal-tree-p value 0))
    (rove:ok (not (%metadata-literal-tree-p (list value) 0))))
  (rove:ok (not (%metadata-literal-tree-p :ok 64))))

(setf (gethash '%metadata-literal-tree-p *unit-hot-probes*)
      (let ((cycle (list :ok)) (deep :ok))
        (dotimes (level 63) (setf deep (list deep)))
        (setf (cdr cycle) cycle)
        (list (list nil 0) (list :ok 0) (list "text" 0) (list (ash 1 100) 0)
              (list (list :nested (list 1 nil "text")) 0)
              (list #(1 2) 0) (list (cons 1 2) 0) (list cycle 0) (list :ok 63) (list :ok 64)
              (list deep 0) (list (list deep) 0)))
      (gethash '%metadata-literal-list-p *unit-hot-probes*)
      (let ((spine (list :ok)) (nested (list nil)))
        (setf (cdr spine) spine (car nested) nested)
        (list (list nil 0) (list (list :ok 1 "text") 1)
              (list (list 1 2 3) 1) (list (cons 1 2) 1)
              (list (list 'car) 1) (list spine 1) (list nested 1)
              (list (list :ok) 63) (list (list :ok) 64))))
