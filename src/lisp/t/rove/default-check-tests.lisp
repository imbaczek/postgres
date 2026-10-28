(in-package #:pg)

(rove:deftest general-default-and-check-parser
    (let* ((ast (first (parse-statements "CREATE TABLE d (a integer DEFAULT 2+3 CHECK(a>0), b integer, CONSTRAINT ordered CHECK(b>=a))")))
           (column (first (getf ast :columns))))
      (rove:ok (getf column :default-expression))
      (rove:ok (= 1 (length (getf column :checks))))
      (rove:ok (equal "ordered" (getf (first (getf ast :constraints)) :name))))
  (dolist (sql '("ALTER TABLE d ALTER COLUMN a SET DEFAULT 4+5"
                 "ALTER TABLE d ALTER a DROP DEFAULT"
                 "ALTER TABLE d ADD CONSTRAINT positive CHECK(a>0) NOT VALID"
                 "ALTER TABLE d VALIDATE CONSTRAINT positive"))
    (rove:ok (parse-statements sql))))

(rove:deftest check-null-passes-and-false-fails
    (let* ((table (make-pg-class :name "d" :oid 42 :columns (vector (make-pg-column :name "a" :type :int4 :typmod -1 :attnum 1))))
           (raw (first (getf (first (parse-statements "CREATE TABLE d(a integer, CHECK(a>0))")) :constraints)))
           (check (cook-check-constraint table raw)))
      (setf (pg-class-checks table) (list check))
      (rove:ok (progn (check-table-constraints table (vector +null+)) t))
      (rove:ok (progn (check-table-constraints table #(1)) t))
      (rove:ok (equal "23514" (handler-case (progn (check-table-constraints table #(0)) nil)
                                (pg-error (e) (pg-error-sqlstate e)))))
      (let ((restored (test-analyze-catalog-class-image-roundtrip table)))
        (rove:ok (equal (pg-class-checks table) (pg-class-checks restored))))))

(rove:deftest decimal-default-catalog-literal-roundtrip
    (let* ((table (make-pg-class :name "decimal_default" :oid 51
                                 :columns (vector (make-pg-column :name "a" :type :int4 :typmod -1 :attnum 1))))
           (ast (first (parse-statements "CREATE TABLE decimal_default(a int DEFAULT '1.5'::numeric CHECK(a<'4.25'::numeric))"))))
      (install-table-expressions table (getf ast :columns) (getf ast :constraints))
      (let* ((restored (test-analyze-catalog-class-image-roundtrip table))
             (column (aref (pg-class-columns restored) 0)))
        (rove:ok (= 2 (insert-column-default column)))
        (rove:ok (equal (catalog-table-definition-sql table) (catalog-table-definition-sql restored))))
      (rove:ok (progn (check-table-constraints table #(4)) t))
      (rove:ok (equal "23514" (handler-case (progn (check-table-constraints table #(5)) nil)
                                (pg-error (e) (pg-error-sqlstate e)))))))

(rove:deftest general-default-analysis-boundaries
    (let ((column (make-pg-column :name "a" :type :int4 :typmod -1 :attnum 1)))
      (dolist (case '(("a+1" "0A000") ("(SELECT 1)" "0A000") ("sum(1)" "42803") ("row_number() OVER()" "42P20")))
        (let* ((ast (first (parse-statements (format nil "CREATE TABLE d(a integer DEFAULT ~a)" (first case)))))
               (raw (getf (first (getf ast :columns)) :default-expression)))
          (rove:ok (equal (second case)
                          (handler-case (progn (cook-column-default raw column) nil)
                            (pg-error (e) (pg-error-sqlstate e)))) (first case))))))

(rove:deftest demanded-default-folding-is-statement-local
    (with-joined-dml-test-catalog
        (let* ((table (lookup-table "dst"))
               (column (aref (pg-class-columns table) 1))
               (raw (getf (first (getf (first (parse-statements
                                               "CREATE TABLE dst(v int DEFAULT CASE WHEN nextval('missing')>0 AND false THEN 9 ELSE 7 END)"))
                                       :columns)) :default-expression)))
          ;; The stored nextval name is deliberately absent; build an analyzed AST
          ;; directly to prove planning removes the call without consulting storage.
          (setf (pg-column-default-expression column)
                (analyze-subquery-assignment (analyze-expr raw nil) column))
          (let* ((prepared (prepare-dml-subqueries (first (parse-statements "INSERT INTO dst(id) VALUES(1)"))))
                 (*dml-column-defaults* (getf prepared :column-defaults)))
            (rove:ok (hash-table-p *dml-column-defaults*))
            (rove:ok (= 7 (coerce-dml-column-default column)))
            (rove:ok (not (eq :const (first (second (pg-column-default-expression column))))))))))

(rove:deftest null-sequence-functions-are-strict-in-check-expressions
    (dolist (sql '("SELECT nextval(NULL)" "SELECT currval(NULL)" "SELECT setval('absent',NULL::bigint)"))
      (let* ((ast (first (parse-statements sql)))
             (expression (analyze-expr (getf (first (getf ast :targets)) :expr) nil)))
        (rove:ok (eq +null+ (eval-expr expression #())) sql))))
