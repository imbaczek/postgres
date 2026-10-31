;;;; sql-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest record-comparisons-check-field-operators
    (flet ((record (type value)
             (make-sql-record :fields (list (list "v" type)) :values (vector value)))
           (failure (thunk)
             (handler-case (progn (funcall thunk) nil)
               (pg-error (condition) (pg-error-sqlstate condition)))))
      (dolist (value (list "{}" +null+))
        (let* ((json (record :json value))
               (nested (record :record json)))
          (dolist (row (list json nested))
            (dolist (operation (list (lambda () (sql-compare "=" row row))
                                     (lambda () (sql-compare "<>" row row))
                                     (lambda () (compare-3way row row))
                                     (lambda () (sql-group-value-key row :record))))
              (rove:ok (equal "42883" (failure operation)))))))
      (let ((jsonb (record :jsonb (sql-jsonb-input "{}")))
            (absent +null+))
        (rove:ok (sql-compare "=" jsonb jsonb))
        (rove:ok (zerop (compare-3way jsonb jsonb)))
        (rove:ok (equal (sql-group-value-key jsonb :record)
                        (sql-group-value-key (record :jsonb (sql-jsonb-input "{}")) :record)))
        (rove:ok (eq +null+ (sql-compare "=" absent absent))))))

(rove:deftest record-comparisons-stop-at-decisive-fields
    (let* ((fields '(("id" :int4) ("j" :json)))
           (left (make-sql-record :fields fields :values #(1 "{}")))
           (right (make-sql-record :fields fields :values #(2 "{}"))))
      (rove:ok (not (sql-compare "=" left right)))
      (rove:ok (sql-compare "<>" left right))
      (rove:ok (minusp (compare-3way left right)))))

(cl-spec:defproperty record-prefix-determines-comparison
    ((prefix (range integer -10000 10000)) (delta (range integer 1 100)))
  (:about compare-sql-records sql-compare compare-3way)
  (:trials (:normal 200))
  (let* ((fields '(("id" :int4) ("j" :json)))
         (left (make-sql-record :fields fields :values (vector prefix +null+)))
         (right (make-sql-record :fields fields :values (vector (+ prefix delta) "{}"))))
    (and (not (sql-compare "=" left right))
         (sql-compare "<>" left right)
         (sql-compare "<" left right)
         (sql-compare ">" right left))))

(rove:deftest generated-record-prefix-comparison
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'record-prefix-determines-comparison :seed 42))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest record-distinct-sorts-with-bounded-memory
    (dolist (memory '(1 100000))
      (let ((*query-work-memory-bytes* memory)
            (*query-temp-directory* (uiop:temporary-directory)))
        (flet ((record (id type value)
                 (make-sql-record :fields (list '("id" :int4) (list "j" type))
                                  :values (vector id value))))
          (dolist (case (list (list (list (record 1 :json "{}")) '(1))
                              (list (list (record 2 :json "{}") (record 1 :json "{}")) '(1 2))
                              (list (list (record 1 :jsonb (sql-jsonb-input "{}"))
                                          (record 1 :jsonb (sql-jsonb-input "{}"))) '(1))))
            (let ((result nil))
              (stream-sorted-distinct
               (lambda (visit)
                 (dolist (record (first case))
                   (let ((values (vector record)))
                     (funcall visit (cons values values)))))
               '((:col 1 :type :record))
               (lambda (row) (push (aref (sql-record-values (aref (car row) 0)) 0) result)))
              (rove:ok (equal (reverse result) (second case)))))
          (let* ((record (record 1 :json "{}"))
                 (values (vector record)))
            (rove:ok
             (equal "42883"
                    (handler-case
                        (stream-sorted-distinct
                         (lambda (visit)
                           (funcall visit (cons values values))
                           (funcall visit (cons values values)))
                         '((:col 1 :type :record)) (lambda (row) (declare (ignore row))))
                      (pg-error (condition) (pg-error-sqlstate condition))))))))))

(rove:deftest test-sql-strict-let-evaluation
    (let ((evaluations '()))
      (rove:ok (eq +null+
                   (sql-strict-let ((left (progn (push :left evaluations) +null+))
                                    (right (progn (push :right evaluations) 7)))
                     :body-must-not-run))
               "strict let returns NULL when any binding is NULL")
      (rove:ok (equal (nreverse evaluations) '(:left :right))
               "strict let evaluates every binding once before NULL check"))
  (let ((evaluations 0))
    (rove:ok (= 9
                (sql-strict-let ((left (progn (incf evaluations) 4))
                                 (right (progn (incf evaluations) 5)))
                  (+ left right)))
             "strict let returns body result for non-NULL bindings")
    (rove:ok (= evaluations 2) "strict let bindings run once each")))

(rove:deftest test-analyzed-function-evaluation-order
    (let ((*backend* (make-backend)))
      (flet ((evaluate (sql)
               (let* ((statement (first (parse-statements sql)))
                      (target (first (getf statement :targets))))
                 (eval-expr (analyze-expr (getf target :expr) nil) nil))))
        (dolist (type '("numeric" "float8"))
          (session-set-application-name "trace")
          (rove:ok
           (eq +null+
               (evaluate
                (format nil
                        "SELECT power(
 CASE WHEN set_config('application_name',
   current_setting('application_name')||':left',false) IS NOT NULL
 THEN NULL::~a END,
 CASE WHEN set_config('application_name',
   current_setting('application_name')||':right',false) IS NOT NULL
 THEN 2::~a END)"
                        type type)))
           "a strict typed call returns NULL")
          (rove:ok (equal "trace:left:right" (sql-setting-value "application_name"))
                   "all arguments run once in order before the strict NULL check"))
        (rove:ok
         (equal "trace:left:right:fallback"
                (evaluate
                 "SELECT set_config('application_name',current_setting('application_name')||':fallback',false)"))
         "ordinary calls retain their side effects and accept SQL false")
        (rove:ok (equal "trace:left:right:fallback"
                        (evaluate "SELECT current_setting('application_name')"))
                 "ordinary calls use the existing function evaluator"))))

(rove:deftest test-sql-current-wal-lsn
    (let* ((*wal-end-lsn* #x123456789)
           (statement (first (parse-statements "SELECT pg_current_wal_lsn()")))
           (target (first (getf statement :targets)))
           (expression (analyze-expr (getf target :expr) nil)))
      (rove:ok (and (eq (expr-type expression) :pg-lsn)
                    (= (eval-expr expression nil) *wal-end-lsn*))
               "pg_current_wal_lsn exposes a typed numeric SQL commit barrier")
      (rove:ok (string= (value->text (eval-expr expression nil) :pg-lsn) "1/23456789")
               "pg_current_wal_lsn retains C text output")))

(rove:deftest test-truncate-parser
    (let ((statement (first (parse-statements "TRUNCATE TABLE first_rows, second_rows"))))
      (rove:ok (and (eq (getf statement :type) :truncate)
                    (equal (getf statement :tables)
                           '("first_rows" "second_rows")))
               "TRUNCATE parses an explicit table list")))

(rove:deftest test-select-top-n-collector
    (let* ((order (list (cons '(:col 1) nil)))
           (collector (make-select-top-n-collector 3 order))
           (input '((5 . :five)
                    (1 . :one-a)
                    (3 . :three)
                    (1 . :one-b)
                    (2 . :two)
                    (0 . :zero)
                    (1 . :one-c))))
      (let ((large-bound (make-select-top-n-collector 1000000000 order)))
        (rove:ok (= (array-dimension (select-top-n-collector-heap large-bound) 0) 32)
                 "ordered LIMIT does not preallocate its full row bound"))
      (dolist (entry input)
        (offer-select-top-n-row
         collector (cons (vector (cdr entry)) (vector (car entry))))
        (rove:ok (<= (length (select-top-n-collector-heap collector)) 3)
                 "ordered LIMIT retains no more than its row bound"))
      (rove:ok (equal (mapcar (lambda (row) (aref (car row) 0))
                              (select-top-n-collector-rows collector))
                      '(:zero :one-a :one-b))
               "ordered LIMIT returns the best rows and preserves ties in input order"))
  (let* ((order (list (cons '(:col 1) nil)))
         (collector (make-select-top-n-collector 40 order)))
    (loop for value from 40 downto 1
          do (offer-select-top-n-row collector
                                     (cons (vector value) (vector value))))
    (rove:ok (= (length (select-top-n-collector-heap collector)) 40)
             "ordered LIMIT grows its heap as candidates arrive")
    (rove:ok (<= (array-dimension (select-top-n-collector-heap collector) 0) 40)
             "ordered LIMIT caps heap capacity at its row bound")
    (rove:ok (equal (mapcar (lambda (row) (aref (car row) 0))
                            (select-top-n-collector-rows collector))
                    (loop for value from 1 to 40 collect value))
             "ordered LIMIT returns the sorted rows after growing its heap"))
  (let* ((order (list (cons '(:col 1) nil)
                      (cons '(:col 2) t)))
         (collector (make-select-top-n-collector 3 order)))
    (dolist (entry '((1 20 :one-20-a)
                     (1 10 :one-10)
                     (2 99 :two-99)
                     (1 20 :one-20-b)))
      (offer-select-top-n-row
       collector
       (cons (vector (third entry))
             (vector (first entry) (second entry)))))
    (rove:ok (equal (mapcar (lambda (row) (aref (car row) 0))
                            (select-top-n-collector-rows collector))
                    '(:one-20-a :one-20-b :one-10))
             "ordered LIMIT supports mixed directions across multiple keys"))
  (let* ((order (list (cons '(:col 1) t)))
         (collector (make-select-top-n-collector 1 order)))
    (dolist (entry (list (cons 1 :one) (cons +null+ :null) (cons 2 :two)))
      (offer-select-top-n-row
       collector (cons (vector (cdr entry)) (vector (car entry)))))
    (rove:ok (equal (mapcar (lambda (row) (aref (car row) 0))
                            (select-top-n-collector-rows collector))
                    '(:null))
             "ordered LIMIT follows DESC NULL ordering")))

(rove:deftest test-using-join-output-metadata
    (let* ((left (make-select-column
                  :name "k" :expr '(:col 1 :type :varchar :typmod 9
                                    :table-oid 11 :attnum 1)
                  :type :varchar :typmod 9 :table-oid 11 :attnum 1))
           (right (make-select-column
                   :name "k" :expr '(:col 1 :type :varchar :typmod 24
                                     :table-oid 22 :attnum 1)
                   :type :varchar :typmod 24 :table-oid 22 :attnum 1))
           (pair (list :left left :right right :type :varchar
                       :typmod (common-using-typmod left right :varchar)))
           (right-output (using-output-column "k" pair :right))
           (full-output (using-output-column "k" pair :full))
           (right-explicit-target
             (analyze-target (list :expr '(:column "k"))
                             (make-select-scope :columns (list right-output))))
           (same-right
             (make-select-column
              :name "k" :expr '(:col 1 :type :varchar :typmod 24
                                :table-oid 22 :attnum 1)
              :type :varchar :typmod 24 :table-oid 22 :attnum 1))
           (same-pair (list :left same-right :right same-right :type :varchar
                            :typmod (common-using-typmod same-right same-right
                                                         :varchar)))
           (same-right-output (using-output-column "k" same-pair :right))
           (same-explicit-target
             (analyze-target (list :expr '(:column "k"))
                             (make-select-scope
                              :columns (list same-right-output))))
           (right-result (target->result-column
                          (select-column-target right-output)))
           (right-explicit-result (target->result-column right-explicit-target))
           (full-result (target->result-column
                         (select-column-target full-output))))
      (rove:ok (= -1 (select-column-typmod right-output))
               "RIGHT USING reports the common typmod for differing inputs")
      (rove:ok (= -1 (result-column-typmod right-result))
               "RIGHT USING protocol metadata carries the common typmod")
      (rove:ok (= -1 (result-column-typmod right-explicit-result))
               "explicit RIGHT USING target carries the common typmod")
      (rove:ok (= 0 (getf right-explicit-target :table-oid))
               "relabelled RIGHT USING target has no base-column origin")
      (rove:ok (= 22 (getf same-explicit-target :table-oid))
               "unrelabelled RIGHT USING target retains its base-column origin")
      (rove:ok (= -1 (result-column-typmod full-result))
               "FULL USING protocol metadata carries the common typmod")
      (let* ((left-int (make-select-column
                        :name "n" :expr '(:col 1 :type :int4 :typmod -1
                                          :table-oid 31 :attnum 1)
                        :type :int4 :typmod -1 :table-oid 31 :attnum 1))
             (right-int (make-select-column
                         :name "n" :expr '(:col 1 :type :int8 :typmod -1
                                           :table-oid 32 :attnum 1)
                         :type :int8 :typmod -1 :table-oid 32 :attnum 1))
             (int-pair (list :left left-int :right right-int
                             :type (common-using-type :int4 :int8 "n")
                             :typmod -1))
             (right-int-output (using-output-column "n" int-pair :right))
             (explicit-int-target
               (analyze-target (list :expr '(:column "n"))
                               (make-select-scope
                                :columns (list right-int-output))))
             (int-result (target->result-column explicit-int-target)))
        (rove:ok (eq :int8 (getf explicit-int-target :type))
                 "explicit USING target is promoted to the common integer type")
        (rove:ok (= +int8oid+ (result-column-type-oid int-result))
                 "promoted USING protocol metadata reports bigint"))))

(rove:deftest test-error-cleanup-and-fatal-rethrow
    (let* ((fatal (make-condition 'pg-error :severity "FATAL"
                                            :sqlstate "XX000" :message "test fatal"))
           (cleanup-count 0)
           (caught nil))
      (handler-case
          (with-error-cleanup (condition (incf cleanup-count))
            (error fatal))
        (pg-error (condition)
          (setf caught condition)))
      (rove:ok (= cleanup-count 1) "error cleanup runs before rethrow")
      (rove:ok (eq caught fatal) "error cleanup re-signals the original condition"))
  (let* ((original (make-condition 'pg-error :severity "ERROR"
                                             :sqlstate "XX000"
                                             :message "original error"))
         (caught nil))
    (handler-case
        (with-error-cleanup (condition (error "injected cleanup failure"))
          (error original))
      (pg-error (condition) (setf caught condition)))
    (rove:ok (eq caught original)
             "cleanup failure does not replace the original condition"))
  (let* ((fatal (make-condition 'pg-error :severity "FATAL"
                                          :sqlstate "XX000" :message "test fatal"))
         (handler-ran nil)
         (caught nil))
    (handler-case
        (handler-case-nonfatal
         (error fatal)
         (condition
          (setf handler-ran (typep condition 'error))))
      (pg-error (condition)
        (setf caught condition)))
    (rove:ok (not handler-ran) "nonfatal handler skips FATAL errors")
    (rove:ok (eq caught fatal) "nonfatal handler re-signals the original FATAL"))
  (rove:ok (eq :handled
               (handler-case-nonfatal
                (error "test nonfatal")
                (condition
                 (if (typep condition 'error) :handled :unexpected))))
           "nonfatal handler handles ordinary errors"))

(rove:deftest test-query-plan-ir-and-count-aggregate
    (let* ((ast (first (parse-statements
                        "select category, count(*) as n from events group by category order by n desc limit 2")))
           (targets (getf ast :targets)))
      (rove:ok (string= (second (first (getf ast :group-by))) "category")
               "SELECT parser retains GROUP BY expressions")
      (rove:ok (eq (first (getf (second targets) :expr)) :count-star)
               "SELECT parser represents COUNT(*) as an aggregate node")
      (rove:ok (= (length (parse-statements "select *, count(*) from events")) 1)
               "SELECT parser accepts a star mixed with other targets")
      (rove:ok (getf (first (parse-statements
                             "select count(*) from events having count(*) > 1"))
                     :having)
               "SELECT parser retains the HAVING qualification")
      (let ((plain (first (parse-statements "explain select 1")))
            (analyze (first (parse-statements "explain analyze select 1")))
            (parenthesized
              (first (parse-statements "explain (analyze) select 1"))))
        (rove:ok (not (getf plain :analyze-p))
                 "EXPLAIN keeps ordinary plans non-executing")
        (rove:ok (getf analyze :analyze-p)
                 "EXPLAIN ANALYZE is retained in the statement IR")
        (rove:ok (getf parenthesized :analyze-p)
                 "EXPLAIN (ANALYZE) is accepted")))
  (let* ((plan (plan-select-query
                (first (parse-statements "select count(*)"))))
         (logical (query-plan-logical plan))
         (physical (query-plan-physical plan))
         (rows '())
         (aggregate-node
           (find :hash-aggregate (collect-query-plan-nodes physical)
                 :key #'first)))
    (rove:ok (eq (first (third logical)) :aggregate)
             "logical plan uses an abstract aggregate operator")
    (rove:ok (eq (first (third physical)) :hash-aggregate)
             "physical plan selects HashAggregate")
    (execute-query-plan plan nil (lambda (row) (declare (ignore row))))
    (rove:ok (zerop (plan-node-metadata-actual-loops
                     (gethash aggregate-node (query-plan-metadata plan))))
             "ordinary execution leaves EXPLAIN ANALYZE counters untouched")
    (let ((*query-plan-instrumentation-p* t))
      (execute-query-plan plan nil
                          (lambda (row) (push (aref (car row) 0) rows))))
    (rove:ok (equal (nreverse rows) '(1))
             "interpreter executes a plain COUNT(*) over its input")
    (rove:ok (= (plan-node-metadata-actual-rows
                 (gethash aggregate-node (query-plan-metadata plan)))
                1)
             "operator sidecar records actual aggregate output rows")
    (let ((metadata (gethash aggregate-node (query-plan-metadata plan))))
      (rove:ok (= (plan-node-metadata-actual-loops metadata) 1)
               "EXPLAIN ANALYZE records one aggregate loop")
      (rove:ok (and (>= (plan-node-metadata-actual-startup-time-ticks metadata) 0)
                    (>= (plan-node-metadata-actual-total-time-ticks metadata) 0))
               "EXPLAIN ANALYZE records nonnegative aggregate timings")
      (let ((formatted (format-query-plan plan nil t 1.25d0)))
        (rove:ok (and (search "(actual time=" formatted)
                      (search "Execution Time: 1.250 ms" formatted))
                 "EXPLAIN ANALYZE formats node and statement timings")))
    (let* ((source-node
             (first (physical-plan-node-children aggregate-node)))
           (root-metadata
             (gethash physical (query-plan-metadata plan)))
           (aggregate-metadata
             (gethash aggregate-node (query-plan-metadata plan)))
           (source-metadata
             (gethash source-node (query-plan-metadata plan))))
      (rove:ok (and source-node root-metadata source-metadata)
               "instrumented aggregate plan has at least three measured levels")
      (rove:ok (>= (plan-node-metadata-actual-total-time-ticks root-metadata)
                   (plan-node-metadata-actual-total-time-ticks aggregate-metadata))
               "EXPLAIN ANALYZE parent timing includes its child exactly once")
      (rove:ok (>= (plan-node-metadata-actual-total-time-ticks aggregate-metadata)
                   (plan-node-metadata-actual-total-time-ticks source-metadata))
               "EXPLAIN ANALYZE aggregate timing includes its input exactly once"))
    (let ((*query-plan-instrumentation-p* t))
      (execute-query-plan plan nil (lambda (row) (declare (ignore row)))))
    (rove:ok (= (plan-node-metadata-actual-loops
                 (gethash aggregate-node (query-plan-metadata plan)))
                1)
             "each instrumented execution resets actual loop counters"))
  (let* ((plan (plan-select-query
                (first (parse-statements "select 7, count(*) where 1 = 0"))))
         (rows '()))
    (execute-query-plan plan nil (lambda (row) (push (car row) rows)))
    (rove:ok (equalp (nreverse rows) (list #(7 0)))
             "global COUNT(*) emits one zero row for empty input and permits constants"))
  (let* ((plan (plan-select-query
                (first (parse-statements "select true, count(*) where 1 = 0"))))
         (rows '()))
    (execute-query-plan plan nil (lambda (row) (push (car row) rows)))
    (rove:ok (equalp (nreverse rows) (list #(t 0)))
             "global COUNT(*) permits row-independent boolean projections"))
  (let* ((group-ref (aggregate-output-ref 1 :int4))
         (targets (list (list :name "category" :expr group-ref)
                        (list :name "category" :expr (copy-tree group-ref))))
         (order (analyze-aggregate-order
                 (list (list :expr '(:column "category") :desc nil))
                 nil targets nil)))
    (rove:ok (equal (caar order) group-ref)
             "ORDER BY accepts duplicate output names for the same expression")))

(rove:deftest test-parameterized-join-streaming-and-reordered-slots
    (let ((rows '()))
      (stream-select-indexed-join-environments
       (lambda (callback)
         (dolist (left (list #(10) #(20)))
           (funcall callback left)))
       (lambda (outer callback)
         (when (= (aref outer 0) 10)
           (funcall callback #(100))))
       (list :type :left :qual '(:bool t) :left-width 1 :right-width 1)
       (lambda (environment) (push environment rows)))
      (rove:ok (equalp (nreverse rows) (list #(10 100) (vector 20 +null+)))
               "parameterized LEFT join emits matches and null-extends misses"))
  (rove:ok (equalp
            (combine-select-join-environment
             #(10 +null+ 30) #(20)
             (list :reordered t :right-offset 1))
            #(10 20 30))
           "greedy join execution writes rows into source-order Var slots"))
