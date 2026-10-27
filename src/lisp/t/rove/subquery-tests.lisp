;;;; subquery-tests.lisp -- query boundaries and expression subplans.
(in-package #:pg)

(rove:deftest subquery-consumer-stop-crosses-limit
    (let ((visited 0))
      (execute-physical-plan-node
       '(:limit 10 (:result)) nil nil
       (lambda (row) (declare (ignore row)) :stop)
       (lambda (node emit)
         (declare (ignore node))
         (loop for value below 20
               do (incf visited)
               when (eq (funcall emit value) :stop) do (return :stop)))
       (lambda (&rest arguments) (declare (ignore arguments))))
      (rove:ok (= visited 1) "the consumer stops LIMIT before its own bound")))

(rove:deftest subquery-consumer-stop-crosses-append
    (let ((opened 0) (visited 0))
      (stream-union-all
       '((() first) (() second))
       (lambda (row) (declare (ignore row)) :stop)
       (lambda (node emit)
         (declare (ignore node))
         (incf opened)
         (incf visited)
         (funcall emit (cons #() #()))))
      (rove:ok (= opened 1) "consumer stop prevents opening the next arm")
      (rove:ok (= visited 1))))

(rove:deftest subquery-parser-forms
    (dolist (sql '("SELECT (SELECT 1)" "SELECT ((SELECT 1))"
                   "SELECT ((SELECT 1) UNION ALL SELECT 2)"
                   "SELECT EXISTS (SELECT 1)" "SELECT NOT EXISTS (SELECT 1)"
                   "SELECT 1 IN (SELECT 1)" "SELECT 1 NOT IN ((SELECT 1))"
                   "SELECT 1 = ANY (SELECT 1)" "SELECT 1 = SOME (SELECT 1)"
                   "SELECT 1 <> ALL (SELECT 1)" "SELECT ARRAY(SELECT 'x')"))
      (rove:ok (eq :select (getf (first (parse-statements sql)) :type)) sql)))

(rove:deftest subquery-parser-expression-continuations
    (dolist (sql '("SELECT 1 IN ((SELECT 1), 2)"
                   "SELECT 2 IN ((SELECT 1)+1)"
                   "SELECT 1 = ANY ((SELECT '{1}'::text[]))"))
      (rove:ok (first (parse-statements sql)) sql))
  (rove:ok (getf (prepare-select (first (parse-statements
                                         "SELECT sum(d.x) = ANY(SELECT 1) FROM (SELECT 1 AS x) d")))
                 :aggregate-p)))

(rove:deftest subquery-analysis-boundaries
    (dolist (sql '("SELECT (SELECT d.x) FROM (SELECT 1 AS x) d"
                   "SELECT (SELECT (SELECT d.x)) FROM (SELECT 1 AS x) d"
                   "SELECT (SELECT count(*) FROM (SELECT 1 AS x) d)"
                   "SELECT sum((SELECT sum(i.x) FROM (SELECT 1 AS x) i))"
                   "SELECT EXISTS(SELECT 1, 2)" "SELECT ARRAY(SELECT 'x')"))
      (let* ((prepared (prepare-select (first (parse-statements sql))))
             (target (first (getf prepared :targets))))
        (rove:ok target sql)))
  (let* ((prepared (prepare-select (first (parse-statements
                                           "SELECT (SELECT 'x' AS child_name)"))))
         (target (first (getf prepared :targets))))
    (rove:ok (string= (getf target :name) "child_name"))
    (rove:ok (eq (getf target :type) :text))
    (rove:ok (zerop (getf target :table-oid)))))

(rove:deftest subquery-analysis-frames-do-not-escape
    (let* ((prepared (prepare-select (first (parse-statements
                                             "SELECT sum((SELECT 1))"))))
           (descriptor (second (getf (first (getf prepared :aggregates)) :input)))
           (child (sql-subplan-prepared descriptor)))
      (rove:ok (null (select-scope-analysis-frame (getf prepared :scope))))
      (rove:ok (null (select-scope-analysis-frame (getf child :scope))))
      (rove:ok (null (select-scope-analysis-frame
                      (select-scope-parent (getf child :scope)))))))

(rove:deftest subquery-dml-analysis-frames-do-not-escape
    (let* ((*catalog* (make-catalog))
           (*backend* (make-backend :database "postgres"))
           (database (make-database :name "postgres" :oid 1))
           (table (make-pg-class
                   :oid 9000 :name "subquery_frame_test" :relfilenode 9000
                   :columns (vector (make-pg-column :name "id" :type :int8
                                                    :typmod -1 :attnum 1)))))
      (setf (gethash "postgres" (catalog-databases *catalog*)) database
            (gethash "subquery_frame_test" (database-tables database)) table)
      (let* ((prepared (prepare-dml-subqueries
                        (first (parse-statements
                                "INSERT INTO subquery_frame_test VALUES ((SELECT sum((SELECT 1))))"))))
             (descriptor (first (getf prepared :sublinks)))
             (child (sql-subplan-prepared descriptor))
             (nested (second (getf (first (getf child :aggregates)) :input))))
        (rove:ok (null (select-scope-analysis-frame (getf prepared :scope))))
        (rove:ok (null (select-scope-analysis-frame (getf child :scope))))
        (rove:ok (null (select-scope-analysis-frame
                        (getf (sql-subplan-prepared nested) :scope)))))))

(rove:deftest subquery-correlated-group-validation
    (let ((prepared (prepare-select (first (parse-statements
                                            "SELECT d.x, (SELECT d.x) FROM (SELECT 1 AS x) d GROUP BY d.x")))))
      (rove:ok (= (length (getf prepared :targets)) 2)))
  (rove:ok (handler-case
               (progn (prepare-select (first (parse-statements
                                              "SELECT count(*), (SELECT d.x) FROM (SELECT 1 AS x) d")))
                      nil)
             (pg-error (condition) (string= (pg-error-sqlstate condition) "42803")))))

(defun subquery-test-rows (sql)
  "Execute virtual-only query fixtures without storage or backend registration."
  (let* ((plan (plan-select-query (first (parse-statements sql))))
         (relations (getf (query-plan-prepared plan) :relations))
         (rows nil))
    (execute-query-plan plan relations
                        (lambda (row) (push (coerce (car row) 'list) rows)))
    (nreverse rows)))

(defun subquery-test-expected (value)
  (cond ((eq value :null) +null+)
        ((consp value) (cons (subquery-test-expected (car value))
                             (subquery-test-expected (cdr value))))
        ((and (vectorp value) (not (stringp value)))
         (map 'vector #'subquery-test-expected value))
        (t value)))

(rove:deftest subquery-execution-results
    (dolist (case '(("SELECT (SELECT 7)" ((7)))
                    ("SELECT (SELECT 7 WHERE false)" ((:null)))
                    ("SELECT EXISTS(SELECT 1 WHERE false), NOT EXISTS(SELECT 1)" ((nil nil)))
                    ("SELECT 1 IN (SELECT 1 UNION ALL SELECT 2)" ((t)))
                    ("SELECT 3 NOT IN (SELECT 1 UNION ALL SELECT NULL)" ((:null)))
                    ("SELECT NULL = ANY (SELECT 1 WHERE false), NULL = ALL (SELECT 1 WHERE false)" ((nil t)))
                    ("SELECT (SELECT d.x+1) FROM (SELECT 4 AS x UNION ALL SELECT 8) d" ((5) (9)))
                    ("SELECT (SELECT (SELECT d.x)) FROM (SELECT 4 AS x) d" ((4)))
                    ("SELECT (SELECT s.x FROM (SELECT d.x AS x) s) FROM (SELECT 4 AS x) d" ((4)))
                    ("SELECT (SELECT count(*) FROM (SELECT 1 AS x) q)" ((1)))
                    ("SELECT sum((SELECT sum(i.x) FROM (SELECT 1 AS x) i))" ((1)))
                    ("SELECT d.x, (SELECT d.x) FROM (SELECT 4 AS x) d GROUP BY d.x" ((4 4)))
                    ("SELECT ARRAY(SELECT 'x' UNION ALL SELECT NULL)" ((#("x" :null))))))
      (let ((expected (subquery-test-expected (second case))))
        (rove:ok (equalp (subquery-test-rows (first case)) expected) (first case)))))

(rove:deftest subquery-scalar-cardinality
    (rove:ok
     (handler-case
         (progn (subquery-test-rows "SELECT (SELECT 1 UNION ALL SELECT 2)") nil)
       (pg-error (condition) (string= (pg-error-sqlstate condition) "21000")))))

(rove:deftest subquery-aggregate-owner
    (rove:ok (equal '((3)) (subquery-test-rows
                            "SELECT (SELECT sum(d.x)) FROM (SELECT 1 AS x UNION ALL SELECT 2) d")))
  (rove:ok (equal '((4)) (subquery-test-rows
                          "SELECT (SELECT sum(d.x)+1) FROM (SELECT 1 AS x UNION ALL SELECT 2) d")))
  (rove:ok (handler-case
               (progn (subquery-test-rows
                       "SELECT sum((SELECT sum(d.x))) FROM (SELECT 1 AS x) d") nil)
             (pg-error (condition) (string= (pg-error-sqlstate condition) "42803"))))
  (rove:ok (handler-case
               (progn (subquery-test-rows
                       "SELECT (SELECT sum(d.x) FROM (SELECT 1 UNION ALL SELECT 2) i) FROM (SELECT 1 AS x) d") nil)
             (pg-error (condition) (string= (pg-error-sqlstate condition) "21000")))))

(rove:deftest subquery-distinct-grouped-output-order
    (rove:ok (equal '((1)) (subquery-test-rows "SELECT DISTINCT count(*) ORDER BY count(*)")))
  (rove:ok (equal '((1 1))
                  (subquery-test-rows
                   "SELECT DISTINCT d.y,count(*) FROM (SELECT 0 AS x,1 AS y) d GROUP BY d.y ORDER BY d.y")))
  (rove:ok (equal '((1 1))
                  (subquery-test-rows
                   "SELECT DISTINCT d.y,(SELECT d.y) AS correlated FROM (SELECT 0 AS x,1 AS y) d GROUP BY d.y ORDER BY correlated"))))

(rove:deftest subquery-distinct-repeated-expression
    (rove:ok (equal '((1))
                    (subquery-test-rows
                     "SELECT DISTINCT (SELECT d.x) FROM (SELECT 1 AS x) d ORDER BY (SELECT d.x)")))
  (rove:ok (equal '((2))
                  (subquery-test-rows
                   "SELECT DISTINCT (SELECT d.x)+1 FROM (SELECT 1 AS x) d ORDER BY (SELECT d.x)+1")))
  (rove:ok (equal '((1))
                  (subquery-test-rows
                   "SELECT DISTINCT (SELECT d.x) FROM (SELECT 1 AS x) d ORDER BY (SELECT x)")))
  (let* ((plan (plan-select-query
                (first (parse-statements
                        "SELECT DISTINCT (SELECT sum(o.x+nextval('unused'))) FROM (SELECT 1 AS x) o ORDER BY (SELECT sum(o.x+nextval('unused')))")))))
    (rove:ok (= 1 (getf (query-plan-prepared plan) :aggregate-count))
             "ORDER reuses the selected outer-owned volatile transition")))

(rove:deftest subquery-semantic-nesting-and-final-dependencies
    (rove:ok (equal '((13))
                    (subquery-test-rows
                     "SELECT (SELECT sum(i.x + sum(o.x)) FROM (SELECT 10 AS x) i) FROM (SELECT 1 AS x UNION ALL SELECT 2) o")))
  (let* ((plan (plan-select-query
                (first (parse-statements
                        "SELECT (SELECT nextval('unused') + CASE WHEN EXISTS(SELECT o.x) THEN 0 ELSE 0 END) FROM (SELECT 1 AS x) o"))))
         (descriptor (first (getf (query-plan-prepared plan) :sublinks))))
    (rove:ok (null (sql-subplan-depths descriptor))
             "EXISTS target removal precedes dependency classification")))

(cl-spec:defspec-function subquery-quantified-step
    (:args (kind (member :any :all))
           (accumulated (member nil t :sql-null))
           (value (member nil t :sql-null)))
  (:returns (member nil t :sql-null))
  (:post
   (eq result
       (if (eq kind :any)
           (cond ((or (eq accumulated t) (eq value t)) t)
                 ((or (eq accumulated +null+) (eq value +null+)) +null+)
                 (t nil))
           (cond ((or (null accumulated) (null value)) nil)
                 ((or (eq accumulated +null+) (eq value +null+)) +null+)
                 (t t))))))

(cl-spec:defproperty subquery-quantified-fold-truth
    ((digits (range integer 0 1048575))
     (size (range integer 0 12))
     (all-p boolean))
  (:about subquery-quantified-step)
  (:trials (:normal 300))
  (let* ((values (loop for index below size
                       collect (aref (vector nil t +null+) (mod (floor digits (expt 3 index)) 3))))
         (kind (if all-p :all :any))
         (actual (reduce (lambda (accumulated value)
                           (subquery-quantified-step kind accumulated value))
                         values :initial-value all-p))
         (expected (if all-p
                       (cond ((member nil values) nil)
                             ((member +null+ values) +null+)
                             (t t))
                       (cond ((member t values) t)
                             ((member +null+ values) +null+)
                             (t nil)))))
    (eq actual expected)))

(cl-spec:defproperty subquery-generated-scalar-cardinality
    ((size (range integer 0 6)) (value (range integer -1000 1000)))
  (:about eval-subquery-expression)
  (:trials (:normal 150))
  (let* ((child (if (zerop size) "SELECT 1 WHERE false"
                    (format nil "~{~a~^ UNION ALL ~}"
                            (loop repeat size collect (format nil "SELECT ~d" value)))))
         (sql (format nil "SELECT (~a)" child)))
    (handler-case
        (let ((rows (subquery-test-rows sql)))
          (and (<= size 1)
               (equal rows (list (list (if (zerop size) +null+ value))))))
      (pg-error (condition)
        (and (> size 1) (string= (pg-error-sqlstate condition) "21000"))))))

(rove:deftest subquery-generated-scalar-cardinality-check
    (unit-check-property 'subquery-generated-scalar-cardinality))

(rove:deftest subquery-generated-quantified-truth
    (let ((data (cl-spec:result-data
                 (cl-spec:check-function 'subquery-quantified-step :trials 300 :seed 42))))
      (rove:ok (eq (getf data :status) :passed) (format nil "~s" data)))
  (unit-check-property 'subquery-quantified-fold-truth))

(rove:deftest subquery-value-and-cache-ownership
    (let* ((record (make-sql-record :fields '((:name "x" :type :text)) :values #("kept")))
           (copy (copy-subquery-value record)))
      (rove:ok (equalp (sql-record-fields copy) (sql-record-fields record)))
      (rove:ok (equalp (sql-record-values copy) #("kept")))
      (rove:ok (not (eq (sql-record-values copy) (sql-record-values record)))))
  (let* ((*subquery-result-cache* (make-hash-table :test #'eq))
         (owner (make-hash-table :test #'eq))
         (*subquery-outer-frames* (list (cons #(7) owner)))
         (descriptor (make-sql-subplan :kind :scalar :depths '(2))))
    (rove:ok (eq owner (subquery-cache-for descriptor)))
    (setf (sql-subplan-depths descriptor) nil)
    (rove:ok (eq *subquery-result-cache* (subquery-cache-for descriptor)))
    (setf (sql-subplan-depths descriptor) '(1))
    (rove:ok (null (subquery-cache-for descriptor)))))

(rove:deftest subquery-explain-and-instrumentation
    (let* ((plan (plan-select-query
                  (first (parse-statements
                          "SELECT (SELECT d.x) FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) d"))))
           (relations (getf (query-plan-prepared plan) :relations))
           (descriptor (first (getf (query-plan-prepared plan) :sublinks)))
           (child (sql-subplan-plan descriptor))
           (*query-plan-instrumentation-p* t))
      (execute-query-plan plan relations (lambda (row) (declare (ignore row))))
      (rove:ok (= 3 (plan-node-metadata-actual-loops
                     (gethash (query-plan-physical child) (query-plan-metadata child)))))
      (rove:ok (search "SubPlan" (format-query-plan plan relations t)))
      (execute-query-plan plan relations (lambda (row) (declare (ignore row))))
      (rove:ok (= 3 (plan-node-metadata-actual-loops
                     (gethash (query-plan-physical child) (query-plan-metadata child))))
               "a new execution resets, while rescans accumulate"))
  (let ((plan (plan-select-query (first (parse-statements "SELECT (SELECT 1)")))))
    (rove:ok (search "InitPlan" (format-query-plan plan)))))

(rove:deftest subquery-inherits-compiler-hook
    (let ((compilations 0))
      (unwind-protect
           (progn
             (register-query-plan-backend
              :subquery-test-compiler
              :compiler (lambda (plan)
                          (incf compilations)
                          (lambda (relations callback)
                            (interpret-query-plan plan relations callback))))
             (let* ((plan (plan-select-query (first (parse-statements "SELECT (SELECT 1)"))))
                    (relations (getf (query-plan-prepared plan) :relations)))
               (execute-query-plan plan relations (lambda (row) (declare (ignore row)))
                                   :backend :subquery-test-compiler)
               (rove:ok (= compilations 2) "root and child inherit the compiler entry")))
        (sb-thread:with-mutex (*query-plan-backend-lock*)
          (remhash :subquery-test-compiler *query-plan-backends*)))))

(rove:deftest subquery-mixed-registration-is-atomic
    (let ((*relation-writers* (make-hash-table :test #'eql))
          (*relation-readers* (make-hash-table :test #'eql))
          (*relation-builders* (make-hash-table :test #'eql))
          (*relation-reader-blockers* (make-hash-table :test #'eql))
          (*relation-build-groups* (make-hash-table :test #'eq))
          (*backend* (make-backend :statement-deadline (1- (get-internal-real-time)))))
      (setf (gethash 20 *relation-reader-blockers*) :blocked)
      (rove:ok (handler-case (progn (register-relation-accesses '(10) '(10 20) 42) nil)
                 (pg-error (condition) (string= (pg-error-sqlstate condition) "57014"))))
      (rove:ok (null (gethash 10 *relation-writers*)) "blocked reads install no partial writer")
      (remhash 20 *relation-reader-blockers*)
      (register-relation-accesses '(10) '(10 20) 42)
      (rove:ok (equal '(42) (gethash 10 *relation-writers*)))
      (rove:ok (null (gethash 10 *relation-readers*)) "self-read needs no read-to-write upgrade")
      (rove:ok (equal '(42) (gethash 20 *relation-readers*)))))

(rove:deftest subquery-version-key-detects-change
    (let ((tuple (make-htup :block 2 :offnum 3 :xmin 4 :xmax 0 :cid 1
                            :ctid-block 2 :ctid-offnum 3 :values #(8))))
      (let ((before (heap-tuple-version-key tuple)))
        (rove:ok (equal before (heap-tuple-version-key tuple)))
        (setf (htup-xmax tuple) 9)
        (rove:ok (equal before (heap-tuple-version-key tuple))
                 "an aborted writer's xmax does not change the target version")
        (setf (htup-offnum tuple) 4)
        (rove:ok (not (equal before (heap-tuple-version-key tuple)))))))
