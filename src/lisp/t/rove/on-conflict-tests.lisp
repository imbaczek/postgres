;;;; on-conflict-tests.lisp -- arbiter namespaces and row-lock lifetime.
(in-package #:pg)

(defun call-with-on-conflict-analysis-fixture (function)
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend :database "postgres"))
         (database (make-database :name "postgres" :oid 1))
         (table (make-pg-class :name "upsert_fixture" :oid 100 :relfilenode 100
                               :indexes '(101 102)
                               :columns (vector
                                         (make-pg-column :name "id" :attnum 1 :type :int4 :typmod -1)
                                         (make-pg-column :name "value" :attnum 2 :type :int4 :typmod -1)))))
    (setf (gethash "postgres" (catalog-databases *catalog*)) database
          (gethash "upsert_fixture" (database-tables database)) table
          (gethash "upsert_fixture_pkey" (database-indexes database))
          (make-pg-index :name "upsert_fixture_pkey" :oid 101 :table-oid 100 :relfilenode 101
                         :columns '(1) :unique-p t :primary-p t :valid-p t)
          (gethash "upsert_fixture_value_key" (database-indexes database))
          (make-pg-index :name "upsert_fixture_value_key" :oid 102 :table-oid 100 :relfilenode 102
                         :columns '(2) :unique-p t :constraint-p t :valid-p t))
    (funcall function table)))

(rove:deftest on-conflict-arbiter-inference
    (call-with-on-conflict-analysis-fixture
     (lambda (table)
       (dolist (case '(("INSERT INTO upsert_fixture VALUES(1,2) ON CONFLICT DO NOTHING" (101 102))
                       ("INSERT INTO upsert_fixture VALUES(1,2) ON CONFLICT(id) DO NOTHING" (101))
                       ("INSERT INTO upsert_fixture VALUES(1,2) ON CONFLICT(id,id) DO NOTHING" (101))
                       ("INSERT INTO upsert_fixture VALUES(1,2) ON CONFLICT ON CONSTRAINT upsert_fixture_value_key DO NOTHING" (102))))
         (let* ((ast (first (parse-statements (first case))))
                (conflict (prepare-on-conflict table (getf ast :on-conflict) nil)))
           (rove:ok (equal (second case) (getf conflict :arbiters)) (first case)))))))

(rove:deftest on-conflict-refresh-targetless-new-index
    (call-with-on-conflict-analysis-fixture
     (lambda (table)
       (let* ((ast (first (parse-statements
                           "INSERT INTO upsert_fixture VALUES(1,2) ON CONFLICT DO NOTHING")))
              (conflict (prepare-on-conflict table (getf ast :on-conflict) nil))
              (database (current-database)))
         (rove:ok (equal '(101 102) (sort (copy-list (getf conflict :arbiters)) #'<)))
         ;; An index build can publish while a reader waits to upgrade to writer.
         (setf (gethash "published_id_key" (database-indexes database))
               (make-pg-index :name "published_id_key" :oid 103 :table-oid 100
                              :relfilenode 103 :columns '(1) :unique-p t :valid-p t)
               (pg-class-indexes table) '(101 102 103))
         (refresh-on-conflict-arbiters table conflict)
         (rove:ok (equal '(101 102 103)
                         (sort (copy-list (getf conflict :arbiters)) #'<))
                  "Targetless DO NOTHING includes the newly published index")))))

(rove:deftest on-conflict-refresh-inferred-index-replacement
    (call-with-on-conflict-analysis-fixture
     (lambda (table)
       (let* ((ast (first (parse-statements
                           "INSERT INTO upsert_fixture VALUES(1,2) ON CONFLICT(id,id) DO UPDATE SET value=excluded.value")))
              (conflict (prepare-on-conflict table (getf ast :on-conflict) nil))
              (sets (getf conflict :sets))
              (targets (getf conflict :targets)))
         (setf (gethash "replacement_id_key" (database-indexes (current-database)))
               (make-pg-index :name "replacement_id_key" :oid 103 :table-oid 100
                              :relfilenode 103 :columns '(1) :unique-p t :valid-p t)
               (pg-class-indexes table) '(102 103))
         (refresh-on-conflict-arbiters table conflict)
         (rove:ok (equal '(103) (getf conflict :arbiters))
                  "Inference resolves the replacement index from stored attnums")
         (rove:ok (eq sets (getf conflict :sets)))
         (rove:ok (eq targets (getf conflict :targets))
                  "Refreshing arbiters preserves the analyzed expression objects")
         (setf (pg-class-indexes table) '(102))
         (let ((caught nil))
           (handler-case (refresh-on-conflict-arbiters table conflict)
             (pg-error (condition) (setf caught (pg-error-sqlstate condition))))
           (rove:ok (equal "42P10" caught)
                    "Dropping the final matching index fails before mutation"))))))

(rove:deftest on-conflict-refresh-constraint-ownership
    (call-with-on-conflict-analysis-fixture
     (lambda (table)
       (let* ((ast (first (parse-statements
                           "INSERT INTO upsert_fixture VALUES(1,2) ON CONFLICT ON CONSTRAINT upsert_fixture_value_key DO NOTHING")))
              (conflict (prepare-on-conflict table (getf ast :on-conflict) nil)))
         (setf (gethash "upsert_fixture_value_key" (database-indexes (current-database)))
               (make-pg-index :name "upsert_fixture_value_key" :oid 104 :table-oid 100
                              :relfilenode 104 :columns '(2) :unique-p t
                              :constraint-p t :valid-p t)
               (pg-class-indexes table) '(101 104))
         (refresh-on-conflict-arbiters table conflict)
         (rove:ok (equal '(104) (getf conflict :arbiters)))
         (setf (pg-index-constraint-p
                (gethash "upsert_fixture_value_key" (database-indexes (current-database))))
               nil)
         (let ((caught nil))
           (handler-case (refresh-on-conflict-arbiters table conflict)
             (pg-error (condition) (setf caught (pg-error-sqlstate condition))))
           (rove:ok (equal "42704" caught)
                    "A same-named standalone unique index is not a constraint"))))))

(rove:deftest on-conflict-target-and-excluded-namespaces
    (call-with-on-conflict-analysis-fixture
     (lambda (table)
       (let* ((ast (first (parse-statements
                           "INSERT INTO upsert_fixture AS target VALUES(1,2) ON CONFLICT(id) DO UPDATE SET value=target.value+excluded.value WHERE excluded.id=target.id")))
              (conflict (prepare-on-conflict table (getf ast :on-conflict) "target"))
              (old #(1 10)) (excluded #(1 3)))
         (rove:ok (equalp #(1 13) (on-conflict-update-values table conflict old excluded)))
         (rove:ok (true-p (eval-expr (getf (getf conflict :where-target) :expr)
                                     (concatenate 'vector old excluded))))
         (rove:ok (= 2 (length (getf conflict :targets)))))
       (let ((scope (on-conflict-expression-scope table nil)) (caught nil))
         (handler-case (analyze-expr '(:column "value") scope)
           (pg-error (condition) (setf caught condition)))
         (rove:ok (and caught (string= "42702" (pg-error-sqlstate caught)))
                  "Unqualified names shared by target and EXCLUDED are ambiguous")))))

(rove:deftest on-conflict-array-partial-assignments
    (call-with-on-conflict-analysis-fixture
     (lambda (table)
       (setf (pg-column-type (aref (pg-class-columns table) 1)) :jsonb-array)
       (let* ((ast (first (parse-statements
                           "INSERT INTO upsert_fixture AS t VALUES(1,ARRAY['1'::jsonb,'2'::jsonb]) ON CONFLICT(id) DO UPDATE SET value[1:1]=ARRAY[excluded.value[2]],value[2]=t.value[1]")))
              (conflict (prepare-on-conflict table (getf ast :on-conflict) "t"))
              (old-array (make-sql-array :element-type-id :jsonb
                                         :elements (vector (sql-jsonb-input "1") (sql-jsonb-input "2"))
                                         :dimensions '(2) :lower-bounds '(1)))
              (excluded-array (make-sql-array :element-type-id :jsonb
                                              :elements (vector (sql-jsonb-input "8") (sql-jsonb-input "9"))
                                              :dimensions '(2) :lower-bounds '(1)))
              (new (on-conflict-update-values table conflict (vector 1 old-array) (vector 1 excluded-array)))
              (elements (sql-array-elements (aref new 1))))
         (rove:ok (string= "9" (sql-jsonb-output (aref elements 0))))
         (rove:ok (string= "1" (sql-jsonb-output (aref elements 1))))
         (rove:ok (string= "1" (sql-jsonb-output (aref (sql-array-elements old-array) 0))))))))

(rove:deftest on-conflict-excluded-alias-is-ambiguous
    (call-with-on-conflict-analysis-fixture
     (lambda (table)
       (let ((scope (on-conflict-expression-scope table "excluded")) (caught nil))
         (handler-case (analyze-expr '(:column "excluded" "value") scope)
           (pg-error (condition) (setf caught condition)))
         (rove:ok (and caught (string= "42P09" (pg-error-sqlstate caught))))))))

(rove:deftest on-conflict-row-lock-lifetime
    (let ((*on-conflict-row-locks* (make-hash-table :test #'equal))
          (*on-conflict-xid-row-locks* (make-hash-table :test #'eql))
          (*active-xids* '(11 12)))
      (rove:ok (null (claim-on-conflict-row-lock 100 0 1 11)))
      (rove:ok (= 11 (claim-on-conflict-row-lock 100 0 1 12)))
      (rove:ok (null (claim-on-conflict-row-lock 100 0 2 12)))
      (release-on-conflict-row-locks 11)
      (rove:ok (null (on-conflict-row-lock-owner 100 0 1 12)))
      (rove:ok (null (claim-on-conflict-row-lock 100 0 1 12)))
      (release-on-conflict-row-locks 12)
      (rove:ok (zerop (hash-table-count *on-conflict-row-locks*)))
      (rove:ok (zerop (hash-table-count *on-conflict-xid-row-locks*)))))

(rove:deftest on-conflict-stale-lock-owner-does-not-remove-successor
    (let ((*on-conflict-row-locks* (make-hash-table :test #'equal))
          (*on-conflict-xid-row-locks* (make-hash-table :test #'eql))
          (*active-xids* '(11 12)))
      (claim-on-conflict-row-lock 100 0 1 11)
      (setf *active-xids* '(12))
      (rove:ok (null (claim-on-conflict-row-lock 100 0 1 12)))
      (release-on-conflict-row-locks 11)
      (rove:ok (= 12 (gethash '(100 0 1) *on-conflict-row-locks*)))
      (release-on-conflict-row-locks 12)
      (rove:ok (zerop (hash-table-count *on-conflict-row-locks*)))))
