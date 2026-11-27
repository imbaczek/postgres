(in-package #:pg)

(rove:deftest joined-dml-source-shifts-only-local-column-operands
    (let* ((source '(:col 2 :type :int4))
           (literal (list :const source :type :text))
           (outer (list :outer-ref 1 source))
           (expression (list :func "f" (list source literal outer) :saved source))
           (shifted (shift-dml-source-expression expression 3)))
      (destructuring-bind (column unchanged-literal unchanged-outer) (third shifted)
        (rove:ok (equal column '(:col 5 :type :int4)))
        (rove:ok (= 2 (second source)))
        (rove:ok (eq source (second unchanged-literal)))
        (rove:ok (eq source (third unchanged-outer)))
        (rove:ok (eq source (getf (cdddr shifted) :saved))))))

(rove:deftest joined-dml-parser
    (dolist (sql '("UPDATE dst d SET v=s.v FROM src s WHERE d.id=s.id RETURNING d.*,s.*"
                   "DELETE FROM dst d USING src s WHERE d.id=s.id RETURNING *"))
      (let ((ast (first (parse-statements sql))))
        (rove:ok (equal "src" (getf (getf ast :dml-source) :from)))
        (rove:ok (equal "d" (getf ast :alias)))
        (rove:ok (getf ast :returning)))))

(defmacro with-joined-dml-test-catalog (&body body)
  `(let* ((*catalog* (make-catalog))
          (*backend* (make-backend :database "postgres"))
          (database (make-database :name "postgres" :oid 1)))
     (setf (gethash "postgres" (catalog-databases *catalog*)) database)
     (loop for name in '("dst" "src") for oid from 9001
           do (setf (gethash name (database-tables database))
                    (make-pg-class :oid oid :name name :relfilenode oid
                                   :columns (vector (make-pg-column :name "id" :type :int4 :attnum 1)
                                                    (make-pg-column :name "v" :type :int4 :attnum 2)))))
     ,@body))

(rove:deftest joined-dml-analysis-namespaces
    (with-joined-dml-test-catalog
        (let* ((prepared (prepare-dml-subqueries
                          (first (parse-statements
                                  "UPDATE dst d SET v=s.v FROM src s WHERE d.id=s.id RETURNING *,old.v,new.v"))))
               (targets (getf prepared :returning)))
          (rove:ok (getf prepared :dml-source-plan))
          (rove:ok (equal '(1 2 7 8 4 6)
                          (mapcar (lambda (target) (second (getf target :expr))) targets)))
          (rove:ok (null (select-scope-analysis-frame (getf prepared :scope)))))))

(rove:deftest joined-dml-hidden-target-and-ambiguity
    (with-joined-dml-test-catalog
        (dolist (case '(("UPDATE dst d SET v=q.id FROM (SELECT d.id) q" . "42P01")
                        ("UPDATE dst d SET v=q.id FROM LATERAL(SELECT d.id) q" . "42P10")
                        ("DELETE FROM dst d USING LATERAL(SELECT id) q" . "42P10")
                        ("UPDATE dst d SET v=id FROM src s" . "42702")
                        ("DELETE FROM dst d USING src d" . "42712")))
          (rove:ok (equal (cdr case)
                          (handler-case (progn (prepare-dml-subqueries (first (parse-statements (car case)))) nil)
                            (pg-error (error) (pg-error-sqlstate error))))
                   (car case)))))

(rove:deftest joined-dml-shift-preserves-merged-columns
    (let* ((expression '(:coalesce ((:col 1 :type :int4) (:col 3 :type :int4)) :type :int4))
           (shifted (shift-dml-source-expression expression 6)))
      (rove:ok (equal shifted '(:coalesce ((:col 7 :type :int4) (:col 9 :type :int4)) :type :int4)))
      (rove:ok (= 1 (second (first (second expression)))))))

(rove:deftest prepared-temporal-qualification-preserves-short-circuit
    (let* ((test '(:binop "<" (:col 1 :type :date)
                   (:binop "+" (:const 0 :type :date) (:const 2147483647 :type :int4)
                    :type :date)))
           (qual (prepared-row-qualification (list :and
                                                   '(:binop "=" (:col 2 :type :int4) (:const 1 :type :int4))
                                                   test))))
      (rove:ok (functionp qual))
      (rove:ok (null (funcall qual #(0 0))))
      (rove:ok (rove:signals (funcall qual #(0 1)) 'pg-error))))

(rove:deftest prepared-row-membership-preserves-sql-null
    (dolist (negated-p '(nil t))
      (let* ((expression
               (list :in nil nil negated-p
                     :array-left '(:col 1 :type :int8)
                     :array-items (list '(:const 3 :type :int8) (list :const +null+))))
             (qualification (prepared-row-qualification expression)))
        (rove:ok (functionp qualification))
        (dolist (value (list 3 4 +null+))
          (let ((row (vector value)))
            (rove:ok (eq (funcall qualification row) (eval-in-expression expression row))))))))

(rove:deftest raw-heap-equality-retains-signed-and-inline-text-matches
    (dolist (case (list (list :int2 -101 -100)
                        (list :int4 -101 -100)
                        (list :int8 -101 -100)
                        (list :float4 -2.25f0 -2.5f0)
                        (list :float8 -2.25d0 -2.5d0)
                        (list :numeric (make-sql-decimal 12500 4) (make-sql-decimal -12500 4))
                        (list :interval (make-sql-interval -1 0 1125000)
                              (make-sql-interval -1 0 1125001))
                        (list :timetz (make-sql-timetz 10000000 -60)
                              (make-sql-timetz 70000000 -120))
                        (list :uuid (sql-uuid-input "adefa715-f641-4862-9bb0-041ac8dcfd51")
                              (sql-uuid-input "adefa715-f641-4862-9bb0-041ac8dcfd50"))
                        (list :varchar (make-string 127 :initial-element #\a)
                              (concatenate 'string (make-string 126 :initial-element #\a) "b"))
                        (list :varchar "café" "cafe")))
      (destructuring-bind (type wanted other) case
        (let* ((columns (vector (make-pg-column :name "id" :type type :attnum 1)))
               (table (make-pg-class :columns columns))
               (expression (list :binop "=" (list :col 1 :type type)
                                 (list :const wanted :type type)))
               (filter (prepared-single-column-heap-filter table expression)))
          (rove:ok (functionp filter))
          (dolist (value (if (eq type :interval)
                             (list wanted other +null+ (make-sql-interval 0 -30 1125000))
                             (list wanted other +null+)))
            (let ((tuple (form-heap-tuple (vector value) columns 3 0 0 0 1)))
              (rove:ok (eq (not (null (funcall filter tuple 0 (length tuple)
                                               (page-u16 tuple +heap-tuple-infomask-off+))))
                           (true-p (eval-expr expression (vector value)))))))))))
