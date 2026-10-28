(in-package #:pg)

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
