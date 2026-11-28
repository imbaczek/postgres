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
        (rove:ok (equalp (deform-heap-tuple old-tuple new-columns)
                         (deform-heap-tuple old-tuple new-columns :metadata :none))
                 "values-only deformation preserves defaults from the current descriptor")
        (rove:ok (equalp new-row (deform-heap-tuple new-tuple new-columns))
                 "physically present NULLs take precedence over missing values")
        (rove:ok (equalp (subseq new-row 0 8) (deform-heap-tuple new-tuple old-columns))
                 "a narrower descriptor reads only its prefix but checks the physical bitmap"))))

(rove:deftest fixed-heap-row-layout-respects-tuple-boundaries
    (let* ((columns (map 'vector (lambda (type) (make-pg-column :type type))
                         '(:int4 :int8 :uuid)))
           (layout (make-fixed-heap-row-layout columns))
           (row (vector -5 4294967298 (sql-uuid-input "01234567-89ab-cdef-0123-456789abcdef")))
           (tuple (form-heap-tuple row columns 1 0 0 0 1))
           (start 13)
           (limit (+ start (length tuple)))
           (page (make-array limit :element-type '(unsigned-byte 8)))
           (nullable (form-heap-tuple (vector +null+ (aref row 1) (aref row 2))
                                      columns 1 0 0 0 1))
           (older (form-heap-tuple (subseq row 0 2) (subseq columns 0 2) 1 0 0 0 1)))
      (replace page tuple :start1 start)
      (rove:ok (equalp row (decode-fixed-heap-row page start limit 0 layout)))
      (rove:ok (null (decode-fixed-heap-row nullable 0 (length nullable) +heap-hasnull+ layout)))
      (rove:ok (null (decode-fixed-heap-row older 0 (length older) 0 layout)))
      (rove:ok (equal "XX001"
                      (handler-case
                          (progn (decode-fixed-heap-row page start (1- limit) 0 layout) nil)
                        (pg-error (condition) (pg-error-sqlstate condition)))))))

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
