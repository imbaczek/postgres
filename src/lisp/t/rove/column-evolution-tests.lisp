;;;; column-evolution-tests.lisp -- heap and descriptor compatibility.

(in-package #:pg)

(rove:deftest heap-deforms-the-physical-attribute-prefix
    (let* ((old-columns (map 'vector (lambda (attnum)
                                       (make-pg-column :type :int4 :attnum attnum))
                             '(1 2 3 4 5 6 7 8)))
           (added (vector (make-pg-column :type :bool :attnum 9 :missing-value nil)
                          (make-pg-column :type :text :attnum 10 :missing-value "original")
                          (make-pg-column :type :int4 :attnum 11)))
           (new-columns (concatenate 'vector old-columns added))
           (old-row (make-array 8 :initial-element +null+))
           (new-row (make-array 11 :initial-element +null+)))
      (setf (aref old-row 7) 42
            (aref new-row 7) 43
            (aref new-row 10) 44)
      (let ((old-tuple (form-heap-tuple old-row old-columns 1 0 0 0 1))
            (new-tuple (form-heap-tuple new-row new-columns 1 0 0 0 1)))
        (rove:ok (equalp (concatenate 'vector old-row (vector nil "original" +null+))
                         (deform-heap-tuple old-tuple new-columns))
                 "an older tuple has one bitmap byte and three missing attributes")
        (rove:ok (equalp new-row (deform-heap-tuple new-tuple new-columns))
                 "physically present NULLs take precedence over missing values")
        (rove:ok (equalp (subseq new-row 0 8) (deform-heap-tuple new-tuple old-columns))
                 "a narrower descriptor reads only its prefix but checks the physical bitmap"))))

(rove:deftest catalog-preserves-typed-missing-column-values
    (let* ((column (make-pg-column :name "flag" :type :bool :attnum 9
                                   :missing-value nil))
           (encoded (column-to-sexp column))
           (defaultless (make-pg-column :name "extra" :type :int4 :attnum 10)))
      (rove:ok (member :missing-value encoded)
               "a missing FALSE must be encoded even though its Lisp value is NIL")
      (rove:ok (null (pg-column-missing-value (column-from-sexp encoded))))
      (rove:ok (not (member :missing-value (column-to-sexp defaultless))))
      (rove:ok (eq +null+ (pg-column-missing-value
                           (column-from-sexp (column-to-sexp defaultless)))))))

(rove:deftest dropped-column-slots-are-hidden-from-sql-scope
    (let* ((columns (vector (make-pg-column :name "a" :type :int4 :attnum 1)
                            (make-pg-column :name "gone" :type :text :attnum 2 :dropped-p t)
                            (make-pg-column :name "b" :type :int4 :attnum 3)))
           (table (make-pg-class :name "example" :oid 100 :columns columns))
           (scope (make-base-select-scope "example" nil table))
           (visible (select-scope-columns scope)))
      (rove:ok (equal '("a" "b") (mapcar #'select-column-name visible)))
      (rove:ok (= 3 (select-scope-width scope)))
      (rove:ok (= 3 (second (select-column-expr (second visible)))))
      (rove:ok (null (find-column table "gone")))
      (rove:ok (null (search "gone" (catalog-table-definition-sql table))))))
