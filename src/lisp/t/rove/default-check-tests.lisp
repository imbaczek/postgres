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

(rove:deftest stored-call-order-diagnostics
    (let ((column (make-pg-column :name "a" :type :int4 :typmod -1 :attnum 1))
          (table (make-pg-class :name "d" :oid 42
                                :columns (vector (make-pg-column :name "a" :type :int4 :typmod -1 :attnum 1)))))
      (dolist (case '(("sin(1 ORDER BY missing_column)" "42809")
                      ("sin(missing_column ORDER BY 1)" "0A000")
                      ("sin(1, 2 ORDER BY missing_column)" "42883")
                      ("sin(DISTINCT 1 ORDER BY missing_column)" "42809")
                      ("sin(sin(1 ORDER BY missing_column))" "42809")))
        (let* ((ast (first (parse-statements (format nil "CREATE TABLE d(a integer DEFAULT ~a)" (first case)))))
               (raw (getf (first (getf ast :columns)) :default-expression)))
          (rove:ok (equal (second case)
                          (handler-case (progn (cook-column-default raw column) nil)
                            (pg-error (e) (pg-error-sqlstate e))))
                   (first case))))
      (let* ((ast (first (parse-statements "CREATE TABLE d(a int, CHECK(sin(1 ORDER BY missing.column) > 0))")))
             (raw (first (getf ast :constraints))))
        (rove:ok (equal "42809"
                        (handler-case (progn (cook-check-constraint table raw) nil)
                          (pg-error (e) (pg-error-sqlstate e))))))))

(rove:deftest demanded-default-folding-is-statement-local
    (with-joined-dml-test-catalog
        (setf (gethash "default_fold_seq" (database-sequences database))
              (make-pg-sequence :name "default_fold_seq" :oid 9901))
      (let* ((table (lookup-table "dst"))
             (column (aref (pg-class-columns table) 1))
             (raw (getf (first (getf (first (parse-statements
                                             "CREATE TABLE dst(v int DEFAULT CASE WHEN nextval('default_fold_seq')>0 AND false THEN 9 ELSE 7 END)"))
                                     :columns)) :default-expression)))
        ;; The name resolves during analysis. Planning still removes the
        ;; unreachable call without advancing the existing sequence.
        (setf (pg-column-default-expression column)
              (analyze-subquery-assignment (analyze-expr raw nil) column))
        (let* ((prepared (prepare-dml-subqueries (first (parse-statements "INSERT INTO dst(id) VALUES(1)"))))
               (*dml-column-defaults* (getf prepared :column-defaults)))
          (rove:ok (hash-table-p *dml-column-defaults*))
          (rove:ok (= 7 (coerce-dml-column-default column)))
          (rove:ok (not (eq :const (first (second (pg-column-default-expression column))))))))))

(rove:deftest null-sequence-functions-are-strict-in-check-expressions
    (with-joined-dml-test-catalog
        (let ((sequence (make-pg-sequence :name "strict_seq" :oid 9902
                                          :last-value 37 :called-p nil)))
          (setf (gethash "strict_seq" (database-sequences database)) sequence)
          (dolist (sql '("SELECT nextval(NULL)" "SELECT currval(NULL)"
                         "SELECT setval('strict_seq',NULL::bigint)"
                         "SELECT setval('strict_seq',NULL)"
                         "SELECT setval('strict_seq',99,NULL)"
                         "SELECT setval('absent'::text,NULL::bigint)"))
            (let* ((ast (first (parse-statements sql)))
                   (expression (analyze-expr (getf (first (getf ast :targets)) :expr) nil)))
              (rove:ok (eq +null+ (eval-expr expression #())) sql)))
          (rove:ok (= 37 (pg-sequence-last-value sequence)))
          (rove:ok (not (pg-sequence-called-p sequence)))
          (rove:ok (zerop (hash-table-count (backend-sequence-currvals *backend*)))))))

(rove:deftest sequence-name-literals-resolve-during-analysis
    (with-joined-dml-test-catalog
        (dolist (sql '("SELECT nextval('absent')"
                       "SELECT currval('absent')"
                       "SELECT setval('absent',NULL::bigint)"
                       "SELECT setval('absent',NULL)"
                       "SELECT CASE WHEN false THEN nextval('absent') ELSE 7 END"
                       "SELECT 1 HAVING false AND nextval('absent')>0"
                       "SELECT 1 HAVING true OR currval('absent')>0"))
          (rove:ok
           (equal "42P01"
                  (handler-case (progn (plan-select-query (first (parse-statements sql))) nil)
                    (pg-error (condition) (pg-error-sqlstate condition))))
           sql))
      (let* ((ast (first (parse-statements
                          "CREATE TABLE d(a int DEFAULT CASE WHEN false THEN nextval('absent') ELSE 7 END)")))
             (raw (getf (first (getf ast :columns)) :default-expression)))
        (rove:ok
         (equal "42P01"
                (handler-case
                    (progn (cook-column-default raw (make-pg-column :name "a" :type :int4 :typmod -1 :attnum 1)) nil)
                  (pg-error (condition) (pg-error-sqlstate condition))))))
      (dolist (case '(("SELECT CASE WHEN false THEN nextval('absent'::text) ELSE 7 END" ((7)))
                      ("SELECT 1 HAVING false AND nextval('absent'::text)>0" nil)
                      ("SELECT 1 HAVING true OR currval('absent'::text)>0" ((1)))
                      ("SELECT CASE WHEN false THEN nextval('dst') ELSE 7 END" ((7)))
                      ("SELECT CASE WHEN false THEN nextval('123456789') ELSE 7 END" ((7)))
                      ("SELECT CASE WHEN false THEN currval('-') ELSE 7 END" ((7)))
                      ("SELECT setval('123456789',NULL::bigint)" ((:sql-null)))))
        (rove:ok (equalp (second case) (subquery-test-rows (first case))) (first case)))
      (dolist (case '(("SELECT CASE WHEN false THEN nextval('4294967296') ELSE 7 END" "22003")
                      ("SELECT setval('0008',NULL::bigint)" "22P02")))
        (rove:ok (equal (second case)
                        (handler-case (progn (plan-select-query (first (parse-statements (first case)))) nil)
                          (pg-error (condition) (pg-error-sqlstate condition))))
                 (first case)))
      (dolist (sql '("SELECT nextval(DISTINCT 'absent')"
                     "SELECT nextval('absent' ORDER BY 1)"
                     "SELECT setval(DISTINCT 'absent','bad')"))
        (rove:ok (equal "42809"
                        (handler-case (progn (plan-select-query (first (parse-statements sql))) nil)
                          (pg-error (condition) (pg-error-sqlstate condition))))
                 sql))
      (rove:ok
       (equal "42809"
              (handler-case (progn (subquery-test-rows "SELECT nextval('dst')") nil)
                (pg-error (condition) (pg-error-sqlstate condition)))))))
