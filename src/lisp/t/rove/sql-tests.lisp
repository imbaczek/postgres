;;;; sql-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest query-plan-walker-follows-only-declared-child-slots
    (let* ((opaque (list :index-scan 999 :literal))
           (seed (list :scan 1))
           (work (list :worktable-scan 2))
           (derived (list :subquery-scan 3 opaque work))
           (recursive (list :recursive-union opaque seed derived))
           (cte (list :cte-scan 4))
           (join (list :join opaque recursive cte))
           (aggregate (list :aggregate opaque opaque join))
           (distinct (list :distinct opaque aggregate))
           (append-node (list :append (list (list :arm distinct opaque)
                                            (list :arm seed opaque))))
           (tree (list :with-ctes (list opaque) append-node))
           (nodes (collect-query-plan-nodes tree))
           (rebased (rebase-select-plan-tree tree 10)))
      (let ((expected (list tree append-node distinct aggregate join
                            recursive seed derived work cte seed)))
        (rove:ok (and (= (length expected) (length nodes))
                      (every #'eq expected nodes))))
      (rove:ok (equal (mapcar #'second (remove-if-not #'query-plan-relation-node-p
                                                      (collect-query-plan-nodes rebased)))
                      '(11 13 12 14 11)))
      (rove:ok (= 999 (second (first (second rebased)))))
      (rove:ok (= 999 (second (third (first (second (third rebased)))))))
      (rove:ok (= 1 (second seed)))))

(rove:deftest boolean-tests-coerce-unknown-operands
    (flet ((evaluate (sql)
             (let* ((ast (first (parse-statements sql)))
                    (expression (getf (first (getf ast :targets)) :expr)))
               (eval-expr (analyze-expr expression nil) nil))))
      (dolist (sql '("SELECT 'true' IS TRUE"
                     "SELECT 'false' IS NOT TRUE"
                     "SELECT NULL IS NOT FALSE"
                     "SELECT FALSE IS FALSE"))
        (rove:ok (eq t (evaluate sql))))
      (dolist (sql '("SELECT 'true' IS FALSE"
                     "SELECT 'false' IS NOT FALSE"
                     "SELECT NULL IS TRUE"))
        (rove:ok (null (evaluate sql))))
      (dolist (case '(("SELECT 0 IS FALSE" "42804")
                      ("SELECT 'garbage' IS FALSE" "22P02")))
        (rove:ok (equal (second case)
                        (handler-case (evaluate (first case))
                          (pg-error (condition) (pg-error-sqlstate condition))))))))

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

(rove:deftest record-grouping-compares-only-reached-fields
    (dolist (memory '(1 100000))
      (let ((*query-work-memory-bytes* memory)
            (*query-temp-directory* (uiop:temporary-directory)))
        (dolist (fixture '(("(SELECT q AS r,id FROM (SELECT 1 AS id,'{}'::json AS j UNION ALL SELECT 2,NULL::json) q) s"
                            (("(1,{})" 1) ("(2,)" 1)) (("(1,{})" 1) ("(2,)" 2)))
                           ("(SELECT q AS r,id FROM (SELECT 1 AS id,NULL::json AS j) q) s"
                            (("(1,)" 1)) (("(1,)" 1)))
                           ("(SELECT q AS r,id FROM (SELECT 1 AS id,'{}'::json AS j) q) s"
                            (("(1,{})" 1)) (("(1,{})" 1)))))
          (loop for aggregate in '("count(*)" "sum(id)")
                for expected in (rest fixture)
                for sql = (format nil "SELECT r::text,~a FROM ~a GROUP BY r ORDER BY r"
                                  aggregate (first fixture))
                do (rove:ok (equal expected (subquery-test-rows sql)) sql)))
        (rove:ok (equal '(("(1,{})" 1) ("(2,)" 2))
                        (subquery-test-rows
                         "WITH s AS (SELECT q AS r,id FROM (SELECT 1 AS id,'{}'::json AS j UNION ALL SELECT 2,NULL::json) q) SELECT r::text,sum(id) FROM s GROUP BY r ORDER BY r")))
        (rove:ok (equal '(("(1,{})" 2))
                        (subquery-test-rows
                         "SELECT r::text,count(*) FROM (SELECT q AS r FROM (SELECT 1 AS id,'{}'::jsonb AS j UNION ALL SELECT 1,'{}'::jsonb) q) s GROUP BY r")))))
  (dolist (json '("'{}'::json" "NULL::json"))
    (let ((sql (format nil "SELECT r,count(*) FROM (SELECT q AS r FROM (SELECT 1 AS id,~a AS j UNION ALL SELECT 1,~a) q) s GROUP BY r" json json)))
      (rove:ok (equal "42883"
                      (handler-case (progn (subquery-test-rows sql) nil)
                        (pg-error (condition) (pg-error-sqlstate condition))))
               sql))))

(cl-spec:defproperty record-grouping-prefix-determines-groups
    ((prefix (range integer -10000 10000)) (delta (range integer 1 100)))
  (:about make-sorted-aggregate-iterator grouping-rows-equal-p)
  (:trials (:normal 100))
  (let* ((fields '(("id" :int4) ("j" :json)))
         (rows (list (vector (make-sql-record :fields fields :values (vector prefix "{}")))
                     (vector (make-sql-record :fields fields :values (vector (+ prefix delta) +null+)))))
         (result nil))
    (map-rove-sorted-aggregate (reverse rows)
                               '((:col 1 :type :record)) '((:kind :count :input nil))
                               (lambda (row) (push (list (aref (sql-record-values (aref row 0)) 0) (aref row 1)) result)))
    (equal (reverse result) (list (list prefix 1) (list (+ prefix delta) 1)))))

(rove:deftest generated-record-grouping-prefix
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'record-grouping-prefix-determines-groups :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 100 (getf data :trials)))))

(rove:deftest record-distinct-lineage-stops-at-slot-and-volatility-boundaries
    (let ((sql "SELECT DISTINCT r FROM (SELECT c AS c,q AS r FROM (SELECT '{}'::json AS j) c CROSS JOIN (SELECT 1 AS id UNION ALL SELECT 2) q) s GROUP BY r"))
      (rove:ok (= 2 (length (subquery-test-rows sql))))))

(rove:deftest record-distinct-volatile-projection-is-a-barrier
    (let ((sql "SELECT DISTINCT r FROM (SELECT q AS r,random() AS x FROM (SELECT '{}'::json AS j) q) s CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2) n"))
      (rove:ok (equal "42883"
                      (handler-case (progn (subquery-test-rows sql) nil)
                        (pg-error (condition) (pg-error-sqlstate condition)))) sql)))

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

(defun map-rove-sorted-distinct (rows keys function)
  (with-query-temp-files ()
                         (let* ((order (mapcar (lambda (key) (list (list :sort-output (second key)))) keys))
                                (sorted (make-select-sort-iterator (make-rove-row-iterator rows) order))
                                (iterator (make-sorted-distinct-iterator sorted keys)))
                           (unwind-protect (map-sql-iterator iterator function)
                             (close-sql-iterator iterator)))))

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
              (map-rove-sorted-distinct
               (mapcar (lambda (record) (let ((row (vector record))) (cons row row))) (first case))
               '((:col 1 :type :record))
               (lambda (row) (push (aref (sql-record-values (aref (car row) 0)) 0) result)))
              (rove:ok (equal (reverse result) (second case)))))
          (let* ((record (record 1 :json "{}"))
                 (values (vector record)))
            (rove:ok
             (equal "42883"
                    (handler-case
                        (map-rove-sorted-distinct
                         (list (cons values values) (cons values values))
                         '((:col 1 :type :record)) (lambda (row) (declare (ignore row))))
                      (pg-error (condition) (pg-error-sqlstate condition))))))))))

(rove:deftest record-distinct-preserves-constant-projection-lineage
    (dolist (sql '("SELECT DISTINCT r FROM (SELECT q AS r FROM (SELECT '{\"a\":1}'::json AS j) q) s CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2) n"
                   "WITH s AS (SELECT q AS r FROM (SELECT '{\"a\":1}'::json AS j) q) SELECT DISTINCT r FROM s CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2) n"
                   "SELECT DISTINCT renamed FROM (SELECT r AS renamed FROM (SELECT q AS r FROM (SELECT '{\"a\":1}'::json AS j) q) s) t CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2) n"
                   "SELECT DISTINCT r FROM (SELECT 7 AS preceding, q AS r FROM (SELECT '{\"a\":1}'::json AS j) q) s CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2) n"))
      (let ((rows (subquery-test-rows sql)))
        (rove:ok (= 1 (length rows)) sql)
        (rove:ok (equal "{\"a\":1}" (aref (sql-record-values (first (first rows))) 0)))))
  (let ((rows (subquery-test-rows "SELECT DISTINCT r, n FROM (SELECT q AS r FROM (SELECT '{\"a\":1}'::json AS j) q) s CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2) n ORDER BY n")))
    (rove:ok (equal '(1 2) (mapcar #'second rows))
             "pruning the constant record retains the varying DISTINCT key"))
  (dolist (sql '("WITH s AS MATERIALIZED (SELECT q AS r FROM (SELECT '{\"a\":1}'::json AS j) q) SELECT DISTINCT r FROM s CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2) n"
                 "SELECT DISTINCT r FROM (SELECT q AS r FROM (SELECT '{\"a\":1}'::json AS j) q LIMIT 1) s CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2) n"
                 "SELECT DISTINCT r FROM (SELECT q AS r FROM (SELECT 1 AS n UNION ALL SELECT 2) n LEFT JOIN (SELECT '{\"a\":1}'::json AS j) q ON n.n = 1) s CROSS JOIN (SELECT 1 AS m UNION ALL SELECT 2) m"))
    (rove:ok (equal "42883"
                    (handler-case (progn (subquery-test-rows sql) nil)
                      (pg-error (condition) (pg-error-sqlstate condition))))
             sql)))

(cl-spec:defproperty record-distinct-projection-depth
    ((depth (range integer 0 6)) (payload (range integer -1000 1000)))
  (:about constant-distinct-record-p)
  (:trials (:normal 40))
  (let ((source (format nil "SELECT q AS r FROM (SELECT '~d'::json AS j) q" payload)))
    (dotimes (index depth)
      (setf source (format nil "SELECT r FROM (~a) alias~d" source index)))
    (let* ((sql (format nil "SELECT DISTINCT r FROM (~a) s CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2) n" source))
           (plan (plan-select-query (first (parse-statements sql))))
           (prepared (query-plan-prepared plan)))
      (null (select-distinct-keys prepared)))))

(rove:deftest generated-record-distinct-projection-lineage
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'record-distinct-projection-depth :seed 42))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 40 (getf result :trials)))))

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

(rove:deftest test-statement-statistics-startup-settings
    (let ((*statement-statistics-store* nil)
          (settings (startup-default-settings "/test")))
      (initialize-statement-statistics settings)
      (rove:ok (null *statement-statistics-store*))
      (setf (gethash "pg_stat_statements.enabled" settings) (list :value t)
            (gethash "pg_stat_statements.max" settings) (list :value 3)
            (gethash "pg_stat_statements.max_text_bytes" settings) (list :value 19))
      (initialize-statement-statistics settings)
      (rove:ok (= 3 (reduce #'+ *statement-statistics-store*
                            :key #'statement-statistics-shard-capacity)))
      (rove:ok (= 19 (reduce #'+ *statement-statistics-store*
                             :key #'statement-statistics-shard-text-budget)))))

(rove:deftest test-statement-statistics-session-track
    (let ((*statement-statistics-store* (make-statement-statistics-store 2 0))
          (*catalog* nil)
          (*backend* (make-backend :startup-user "postgres"))
          (setting "pg_stat_statements.track"))
      (rove:ok (string= "top" (session-extra-setting-value setting)))
      (session-set-extra-setting setting "NONE")
      (rove:ok (string= "none" (session-extra-setting-value setting)))
      (let ((condition (handler-case (progn (session-set-extra-setting setting "invalid") nil)
                         (pg-error (error) error))))
        (rove:ok (and condition (string= "22023" (pg-error-sqlstate condition)))))
      (rove:ok (string= "none" (session-extra-setting-value setting)))
      (session-set-extra-setting setting :default)
      (rove:ok (string= "top" (session-extra-setting-value setting)))
      (let* ((*backend* (make-backend :startup-user "reporter"))
             (condition (handler-case (progn (session-set-extra-setting setting "none") nil)
                          (pg-error (error) error))))
        (rove:ok (and condition (string= "42501" (pg-error-sqlstate condition)))))))

(rove:deftest test-sql-activity-view-row
    (let ((backend (make-backend :startup-user "postgres" :pid 42))
          (*catalog* nil)
          (query (make-string 1100 :initial-element #\x)))
      (unwind-protect
           (progn
             (begin-backend-query-execution backend)
             (publish-backend-activity-query backend query 123)
             (setf (backend-application-name backend) (copy-seq "test client"))
             (with-backend-wait-state (backend backend-activity-wait-event :relation)
               (catch 'interrupted
                 (with-backend-wait-state (backend backend-activity-wait-event :transactionid)
                   (let ((row (sql-activity-view-row backend "postgres" nil))
                         (hidden (sql-activity-view-row backend "observer" nil)))
                     (rove:ok (string= "Lock" (aref row 8)))
                     (rove:ok (string= "transactionid" (aref row 9)))
                     (rove:ok (string= "active" (aref row 10)))
                     (rove:ok (= 123 (aref row 11)))
                     (rove:ok (= 1024 (length (aref row 12))))
                     (setf (char (aref row 12) 0) #\z
                           (char (aref row 4) 0) #\z)
                     (let ((next (sql-activity-view-row backend "postgres" nil)))
                       (rove:ok (char= #\x (char (aref next 12) 0)))
                       (rove:ok (string= "test client" (aref next 4))))
                     (rove:ok (eq +null+ (aref hidden 9)))
                     (rove:ok (eq +null+ (aref hidden 11)))
                     (rove:ok (string= "<insufficient privilege>" (aref hidden 12))))
                   (throw 'interrupted nil)))
               (rove:ok (eq :relation (backend-activity-wait-event backend)))
               (let ((*backend* backend))
                 (call-with-backend-io-wait
                  :data-file-read
                  (lambda ()
                    (let ((row (sql-activity-view-row backend "postgres" nil)))
                      (rove:ok (string= "IO" (aref row 8)))
                      (rove:ok (string= "DataFileRead" (aref row 9)))))))
               (rove:ok (eq :relation (backend-activity-wait-event backend)))))
        (end-backend-query-execution backend))
      (let ((row (sql-activity-view-row backend "postgres" nil)))
        (rove:ok (string= "idle" (aref row 10)))
        (rove:ok (eq +null+ (aref row 9)))
        (rove:ok (>= (aref row 7) (aref row 6))))))

(rove:deftest test-sql-statement-statistics-rows
    (let ((*statement-statistics-store* (make-statement-statistics-store 4 64 :shards 1))
          (*catalog* nil)
          (key '(10 5 t 120)))
      (store-statement-statistics-phase *statement-statistics-store* key :planning 2.0d0 0 "select $1")
      (store-statement-statistics-phase *statement-statistics-store* key :execution 4.0d0 3 nil
                                        (make-statement-resource-usage :relation-reads 2
                                                                       :wal-records 1 :wal-bytes 32))
      (let* ((*backend* (make-backend :startup-user "postgres"))
             (row (first (sql-statement-statistics-rows))))
        (rove:ok (= 54 (length row)))
        (rove:ok (equalp (subseq row 0 18)
                         #(10 5 t 120 "select $1" 1 2.0d0 2.0d0 2.0d0 2.0d0 0.0d0
                           1 4.0d0 4.0d0 4.0d0 4.0d0 0.0d0 3)))
        (rove:ok (every (lambda (value) (eq value +null+)) (subseq row 18 52)))
        (rove:ok (and (integerp (aref row 52)) (= (aref row 52) (aref row 53)))))
      (let ((*backend* (make-backend :startup-user "postgres")))
        (rove:ok (equalp (sql-statement-statistics-resource-rows)
                         (list #(10 5 t 120 2 0 0 0 0 1 32 0 0)))))
      (let* ((*backend* (make-backend :startup-user "reporter"))
             (row (first (sql-statement-statistics-rows))))
        (rove:ok (eq +null+ (aref row 3)))
        (rove:ok (string= "<insufficient privilege>" (aref row 4)))
        (rove:ok (= 3 (aref row 17))))))

(rove:deftest test-sql-statement-statistics-reset
    (let ((*statement-statistics-store* (make-statement-statistics-store 2 0))
          (*catalog* nil))
      (store-statement-statistics-phase *statement-statistics-store* '(10 5 t 120)
                                        :execution 4.0d0 3 nil)
      (let ((*backend* (make-backend :startup-user "reporter")))
        (let ((condition (handler-case (progn (sql-statement-statistics-reset) nil)
                           (pg-error (error) error))))
          (rove:ok (and condition (string= (pg-error-sqlstate condition) "42501"))))
        (rove:ok (= 1 (length (sql-statement-statistics-rows)))))
      (let ((*backend* (make-backend :startup-user "postgres")))
        (let ((reset-time (sql-statement-statistics-reset)))
          (rove:ok (integerp reset-time))
          (rove:ok (equalp (list (vector 0 reset-time)) (sql-statement-statistics-info-rows))))
        (rove:ok (null (sql-statement-statistics-rows))))))

(rove:deftest test-statement-statistics-store
    (let* ((store (make-statement-statistics-store 2 8 :shards 1))
           (first '(1 2 t 3))
           (second '(1 2 t 4))
           (third '(1 2 t 5)))
      (store-statement-statistics-phase store first :planning 2.0d0 0 "a")
      (dolist (elapsed '(3.0d0 1.0d0 5.0d0))
        (store-statement-statistics-phase store first :execution elapsed 1 "a"))
      (let* ((entry (first (snapshot-statement-statistics store)))
             (timing (statement-statistics-entry-execution entry)))
        (rove:ok (= 3 (statement-statistics-entry-rows entry)))
        (rove:ok (= 1 (statement-phase-statistics-count
                       (statement-statistics-entry-planning entry))))
        (rove:ok (= 3 (statement-phase-statistics-count timing)))
        (rove:ok (= 9.0d0 (statement-phase-statistics-total timing)))
        (rove:ok (= 1.0d0 (statement-phase-statistics-minimum timing)))
        (rove:ok (= 5.0d0 (statement-phase-statistics-maximum timing)))
        (rove:ok (= 3.0d0 (statement-phase-statistics-mean timing)))
        (rove:ok (= 8.0d0 (statement-phase-statistics-m2 timing)))
        (setf (char (statement-statistics-entry-text entry) 0) #\z)
        (rove:ok (string= "a" (statement-statistics-entry-text
                               (first (snapshot-statement-statistics store))))))
      (store-statement-statistics-phase store second :execution 1.0d0 1 "b")
      (store-statement-statistics-phase store third :execution 1.0d0 1 "too long")
      (let ((entries (snapshot-statement-statistics store)))
        (rove:ok (= 2 (length entries)))
        (rove:ok (not (find first entries :key #'statement-statistics-entry-key :test #'equal)))
        (rove:ok (null (statement-statistics-entry-text
                        (find third entries :key #'statement-statistics-entry-key :test #'equal)))))
      (rove:ok (= 1 (statement-statistics-info store)))
      (reset-statement-statistics store)
      (rove:ok (zerop (statement-statistics-info store)))
      (rove:ok (null (snapshot-statement-statistics store)))
      (store-statement-statistics-phase store first :execution 7.0d0 1 "a")
      (rove:ok (= 1 (statement-statistics-entry-rows
                     (first (snapshot-statement-statistics store)))))))

(rove:deftest test-statement-statistics-text-is-rendered-on-insertion
    (let ((store (make-statement-statistics-store 1 64 :shards 1))
          (key '(1 2 t 3))
          (renders 0))
      (flet ((record ()
               (store-statement-statistics-phase
                store key :execution 1.0d0 1
                (lambda ()
                  ;; A reset between lookup and publication must complete
                  ;; without the producer holding the shard lock.
                  (reset-statement-statistics store)
                  (incf renders)
                  "select $1"))))
        (record)
        (record)
        (rove:ok (= renders 1))
        (let ((entry (first (snapshot-statement-statistics store))))
          (rove:ok (= 2 (statement-statistics-entry-rows entry)))
          (rove:ok (string= "select $1" (statement-statistics-entry-text entry))))
        (reset-statement-statistics store)
        (record)
        (rove:ok (= renders 2))
        (rove:ok (= 1 (statement-statistics-entry-rows
                       (first (snapshot-statement-statistics store))))))))

(rove:deftest test-statement-statistics-targeted-reset
    (let* ((store (make-statement-statistics-store 3 96 :shards 1))
           (initial-reset (nth-value 1 (statement-statistics-info store)))
           (first '(1 5 t 10))
           (middle '(1 5 t 11))
           (last '(2 5 t 12)))
      (dolist (key (list first middle last))
        (store-statement-statistics-phase store key :execution 2.0d0 1 "select"))
      (reset-statement-statistics store (lambda () 100) 1 5 11 t)
      (store-statement-statistics-phase store middle :execution 6.0d0 1 nil)
      (let* ((entry (find middle (snapshot-statement-statistics store)
                          :key #'statement-statistics-entry-key :test #'equal))
             (phase (statement-statistics-entry-execution entry)))
        (rove:ok (= 2 (statement-phase-statistics-count phase)))
        (rove:ok (= 8.0d0 (statement-phase-statistics-total phase)))
        (rove:ok (= 6.0d0 (statement-phase-statistics-minimum phase)))
        (rove:ok (= 6.0d0 (statement-phase-statistics-maximum phase))))
      (reset-statement-statistics store (lambda () 110) 1 5 11)
      (let ((shard (aref store 0)))
        (rove:ok (equal last (statement-statistics-entry-key (statement-statistics-shard-newest shard))))
        (rove:ok (equal first (statement-statistics-entry-key (statement-statistics-shard-oldest shard))))
        (rove:ok (= 48 (statement-statistics-shard-text-bytes shard))))
      (rove:ok (integerp initial-reset))
      (rove:ok (= initial-reset (nth-value 1 (statement-statistics-info store))))))

(rove:deftest test-normalized-statement-text
    (let ((statements
            (parse-statements "SELECT $2, 'secret', $$private$$, 123, NULL /* comment */; SELECT 8"
                              :collect-statement-text-p t)))
      (rove:ok (equal (mapcar (lambda (ast) (getf ast :normalized-statement-text)) statements)
                      '("select $2 , $3 , $4 , $5 , $6" "select $1")))
      (rove:ok (null (getf (first (parse-statements "SELECT 8")) :normalized-statement-text))))
  (let ((statements (parse-statements "select 'secret'::varchar(8), cast(25 as numeric(4,2))"
                                      :collect-statement-text-p t)))
    (rove:ok (equal "select $1 :: varchar ( 8 ) , cast ( $2 as numeric ( 4 , 2 ) )"
                    (getf (first statements) :normalized-statement-text))))
  (let* ((name (make-string 3000 :initial-element #\x))
         (ast (first (parse-statements (format nil "SELECT 1 AS ~a" name)
                                       :collect-statement-text-p t))))
    (rove:ok (null (getf ast :normalized-statement-text)))))

(rove:deftest test-statement-fingerprint-version-one
    (let ((shape '(:select (16384) ((:col :int4 -1 0))
                   (:binop :bool -1 "=" (:col :int4 -1 0) (:const :int4 -1)) nil nil)))
      (rove:ok (= 1 +statement-fingerprint-version+))
      (rove:ok (= 2044300642419585876 (statement-shape-id shape)))))

(rove:deftest test-statement-select-fingerprint
    (flet ((fingerprint (sql)
             (statement-select-fingerprint (prepare-select (first (parse-statements sql))))))
      (let ((plain (fingerprint "select 1"))
            (cast (fingerprint "select 3::bigint")))
        (rove:ok (integerp plain))
        (rove:ok (eql plain (fingerprint "select 2")))
        (rove:ok (integerp cast))
        (rove:ok (eql cast (fingerprint "select 4::bigint")))
        (rove:ok (not (eql plain cast)))
        (rove:ok (not (eql plain (fingerprint "select 1 limit 1")))))
      (let ((union-all (fingerprint "select 1 union all select 2 order by 1 desc limit 3")))
        (rove:ok (integerp union-all))
        (rove:ok (eql union-all (fingerprint "select 4 union all select 5 order by 1 desc limit 6")))
        (rove:ok (not (eql union-all (fingerprint "select 4 union select 5 order by 1 desc limit 6"))))
        (rove:ok (not (eql union-all (fingerprint "select 4 union all select 5::bigint order by 1 desc limit 6"))))
        (rove:ok (not (eql union-all (fingerprint "select 4 union all select 5 order by 1 asc limit 6")))))
      (let ((derived (fingerprint "select x from (select 1 as x) s")))
        (rove:ok (integerp derived))
        (rove:ok (eql derived (fingerprint "select y from (select 2 as y) t")))
        (rove:ok (not (eql derived (fingerprint "select x from (select 1 + 2 as x) s")))))
      (let ((join (fingerprint "select a.x from (select 1 as x) a join (select 2 as x) b on a.x = b.x")))
        (rove:ok (integerp join))
        (rove:ok (eql join (fingerprint "select p.y from (select 3 as y) p join (select 4 as y) q on p.y = q.y")))
        (rove:ok (not (eql join (fingerprint "select a.x from (select 1 as x) a left join (select 2 as x) b on a.x = b.x"))))
        (rove:ok (not (eql join (fingerprint "select a.x from (select 1 as x) a join (select 2 as x) b on a.x < b.x")))))
      (let ((lower (fingerprint "select lower('abc')")))
        (rove:ok (integerp lower))
        (rove:ok (eql lower (fingerprint "select pg_catalog.lower('def')")))
        (let ((length (fingerprint "select length('abc'::text)"))
              (character-length (fingerprint "select character_length('abc'::text)")))
          (rove:ok (every #'integerp (list length character-length)))
          (rove:ok (not (eql length character-length)))))
      (let ((equality (fingerprint "select 1 where 2 = 3"))
            (ordering (fingerprint "select 1 where 2 > 3")))
        (rove:ok (integerp equality))
        (rove:ok (eql equality (fingerprint "select 4 where 5 = 6")))
        (rove:ok (integerp ordering))
        (rove:ok (not (eql equality ordering))))
      (let ((is-null (fingerprint "select 1 is null"))
            (is-not-null (fingerprint "select 1 is not null"))
            (true (fingerprint "select true")))
        (rove:ok (every #'integerp (list is-null is-not-null true)))
        (rove:ok (not (eql is-null is-not-null)))
        (rove:ok (eql true (fingerprint "select false"))))
      (let ((ordered (fingerprint "select 1 as x order by x desc")))
        (rove:ok (integerp ordered))
        (rove:ok (eql ordered (fingerprint "select 2 order by 1 desc")))
        (rove:ok (not (eql ordered (fingerprint "select 2 order by 1 asc")))))
      (let ((aggregates (fingerprint "select count(*), sum(1), sum(1)")))
        (rove:ok (integerp aggregates))
        (rove:ok (eql aggregates (fingerprint "select count(*), sum(2), sum(3)")))
        (rove:ok (not (eql aggregates (fingerprint "select count(1), sum(2), sum(3)")))))
      (let ((having (fingerprint "select count(*) having count(*) > 1 and 2 > 1")))
        (rove:ok (integerp having))
        (rove:ok (eql having (fingerprint "select count(*) having count(*) > 3 and 4 > 3")))
        (rove:ok (not (eql having
                           (fingerprint "select count(*) where 2 > 1 having count(*) > 1")))))))

(rove:deftest test-statement-fingerprint-rejects-unknown-node
    (let ((unknown '(:unknown-analyzed-node :type :int4 :typmod -1)))
      (rove:ok (null (statement-select-fingerprint (list :targets (list (list :expr unknown))))))
      (rove:ok (null (statement-dml-fingerprint
                      :insert (list :table (make-pg-class :oid 30000)
                                    :attnums '(1)
                                    :rows (list (list (list :assignment unknown :type :int4 :typmod -1)))))))))

(rove:deftest test-statement-dml-fingerprint
    (let ((table (make-pg-class :oid 30000))
          (column (make-pg-column :attnum 1)))
      (flet ((fingerprint (kind value &optional returning)
               (let* ((expression (list :const value :type :int4 :typmod -1))
                      (assignment (list :assignment expression :type :int4 :typmod -1)))
                 (statement-dml-fingerprint
                  kind (list :table table
                             :attnums (when (eq kind :insert) '(1))
                             :rows (when (eq kind :insert) (list (list assignment)))
                             :sets (when (eq kind :update) (list (cons column assignment)))
                             :returning (when returning (list (list :expr expression))))))))
        (let ((insert (fingerprint :insert 1))
              (update (fingerprint :update 1))
              (delete (fingerprint :delete 1)))
          (rove:ok (every #'integerp (list insert update delete)))
          (rove:ok (eql insert (fingerprint :insert 2)))
          (rove:ok (eql update (fingerprint :update 2)))
          (rove:ok (not (eql insert update)))
          (rove:ok (not (eql update delete)))
          (rove:ok (not (eql insert (fingerprint :insert 1 t))))))
      (rove:ok (null (statement-dml-fingerprint :merge (list :table table))))))

(rove:deftest test-statement-observation-lifecycle
    (let* ((store (make-statement-statistics-store 4 64))
           (observation (make-statement-observation :store store :key '(1 2 t 10) :text "select"))
           (ticks 0)
           (*statement-observation-clock* (lambda () ticks)))
      (with-statement-planning ((analyzed (progn (incf ticks 3) :analyzed))
                                (query-id (progn (incf ticks 100) 10))
                                ((first second) (progn (incf ticks 5) (values analyzed :planned)))
                                planning-ticks t)
        (rove:ok (equal (list first second query-id) '(:analyzed :planned 10)))
        (rove:ok (= planning-ticks 8))
        (store-statement-statistics-phase
         store '(1 2 t 10) :planning (statement-observation-milliseconds planning-ticks) 0 "select"))
      (call-with-statement-execution-slice
       observation
       (lambda ()
         (rove:ok (eq *active-statement-observation* observation))
         (incf (statement-resource-usage-relation-reads *statement-resource-usage*) 2)
         (incf ticks 5)
         (incf (statement-observation-rows observation))))
      (rove:ok (null *active-statement-observation*))
      (rove:ok (null *statement-resource-usage*))
      (rove:ok (zerop (statement-phase-statistics-count
                       (statement-statistics-entry-execution
                        (first (snapshot-statement-statistics store))))))
      (reset-statement-statistics store)
      (incf ticks 1000)
      (call-with-statement-execution-slice
       observation
       (lambda ()
         (incf (statement-resource-usage-relation-reads *statement-resource-usage*))
         (incf ticks 7)
         (incf (statement-observation-rows observation))))
      (rove:ok (= 3 (statement-resource-usage-relation-reads
                     (statement-observation-resources observation))))
      (finish-statement-observation observation :success)
      (finish-statement-observation observation :success)
      (let* ((entry (first (snapshot-statement-statistics store)))
             (execution (statement-statistics-entry-execution entry)))
        (rove:ok (= 2 (statement-statistics-entry-rows entry)))
        (rove:ok (= 3 (statement-resource-usage-relation-reads
                       (statement-statistics-entry-resources entry))))
        (rove:ok (= 1 (statement-phase-statistics-count execution)))
        (rove:ok (= (statement-observation-milliseconds 12)
                    (statement-phase-statistics-total execution)))
        (rove:ok (zerop (statement-phase-statistics-count
                         (statement-statistics-entry-planning entry)))))
      (let ((failed (make-statement-observation :store store :key '(1 2 t 11))))
        (catch 'interrupted
          (call-with-statement-execution-slice
           failed (lambda () (incf ticks 20) (throw 'interrupted nil))))
        (rove:ok (eq :failed (statement-observation-terminal-status failed)))
        (finish-statement-observation failed :success)
        (rove:ok (= 1 (length (snapshot-statement-statistics store)))))))

(rove:deftest test-explain-timing-off
    (let* ((ast (first (parse-statements "explain (analyze, timing off) select 1")))
           (plan (plan-select-query (getf ast :statement)))
           (*query-plan-instrumentation-p* :rows))
      (rove:ok (getf ast :analyze-p))
      (rove:ok (null (getf ast :timing-p)))
      (map-query-plan-rows plan nil (lambda (row) (declare (ignore row))))
      (let ((description (format-query-plan plan nil t)))
        (rove:ok (search "actual rows=" description))
        (rove:ok (search "loops=1" description))
        (rove:ok (not (search "actual time=" description))))
      (maphash (lambda (node metadata)
                 (declare (ignore node))
                 (rove:ok (zerop (plan-node-metadata-actual-total-time-ticks metadata))))
               (query-plan-metadata plan)))
  (let ((condition (handler-case
                       (progn (parse-statements "explain (timing on) select 1") nil)
                     (pg-error (error) error))))
    (rove:ok (and condition (string= (pg-error-sqlstate condition) "22023")))))

(rove:deftest test-explain-empty-rescan-average
    (let* ((plan (plan-select-query (first (parse-statements "select 1"))))
           (*cte-active-plan* plan)
           (*query-plan-instrumentation-p* t)
           (clock 0)
           (*statement-observation-clock* (lambda () clock))
           (node (query-plan-physical plan))
           (metadata (gethash node (query-plan-metadata plan))))
      (dolist (present-p '(t nil))
        (let ((iterator (instrument-physical-plan-iterator
                         node (make-sql-iterator
                               (lambda ()
                                 (incf clock (if present-p 3 7))
                                 (values #(1) present-p))))))
          (unwind-protect (sql-iterator-next iterator)
            (close-sql-iterator iterator))))
      (rove:ok (= 2 (plan-node-metadata-actual-loops metadata)))
      (rove:ok (= 1/2 (plan-node-metadata-actual-rows metadata)))
      (rove:ok (= 10 (plan-node-metadata-actual-total-time-ticks metadata)))))

(rove:deftest test-explain-inclusive-resource-deltas
    (let* ((plan (plan-select-query (first (parse-statements "select 1"))))
           (*cte-active-plan* plan)
           (*query-plan-instrumentation-p* t)
           (ticks 0)
           (*statement-observation-clock* (lambda () ticks))
           (*statement-resource-usage* (make-statement-resource-usage))
           (parent (query-plan-physical plan))
           (child (first (physical-plan-node-children parent)))
           (leaf (make-sql-iterator
                  (lambda ()
                    (incf (statement-resource-usage-relation-reads *statement-resource-usage*))
                    (incf (statement-resource-usage-spill-read-bytes *statement-resource-usage*) 3)
                    (incf ticks 4)
                    (values #(1) t))))
           (iterator (instrument-physical-plan-iterator
                      parent (instrument-physical-plan-iterator child leaf))))
      (unwind-protect
           (progn
             (dotimes (pull 2) (sql-iterator-next iterator))
             (rove:ok (= 2 (statement-resource-usage-relation-reads *statement-resource-usage*)))
             (dolist (node (list parent child))
               (let* ((metadata (gethash node (query-plan-metadata plan)))
                      (resources (plan-node-metadata-resources metadata)))
                 (rove:ok (= 4 (plan-node-metadata-actual-startup-time-ticks metadata)))
                 (rove:ok (= 8 (plan-node-metadata-actual-total-time-ticks metadata)))
                 (rove:ok (= 2 (statement-resource-usage-relation-reads resources)))
                 (rove:ok (= 6 (statement-resource-usage-spill-read-bytes resources))))))
        (close-sql-iterator iterator))))

(rove:deftest test-explain-json
    (let* ((ast (first (parse-statements "explain (format 'JSON', analyze +1, timing -0) select 1")))
           (plan (plan-select-query (getf ast :statement)))
           (*query-plan-instrumentation-p* :rows))
      (rove:ok (eq (getf ast :format) :json))
      (rove:ok (not (getf ast :timing-p)))
      (map-query-plan-rows plan nil (lambda (row) (declare (ignore row))))
      (let ((description (format-query-plan-json plan nil t nil 0.25d0 0.125d0)))
        (rove:ok (equal (sql-json-input description) description))
        (rove:ok (search "\"Plans\":[" description))
        (rove:ok (search "\"Actual Rows\":1.00" description))
        (rove:ok (search "\"Actual Loops\":1" description))
        (rove:ok (search "\"Execution Time\":0.250" description))
        (rove:ok (search "\"Planning Time\":0.125" description))
        (rove:ok (search "\"Parent Relationship\":\"Outer\"" description))
        (rove:ok (not (search "\"Actual Total Time\"" description))))))

(rove:deftest test-explain-dependent-plans
    (let* ((*backend* (make-backend))
           (statement (first (parse-statements
                              "with q as materialized (select 2 as x) select (select x from q)")))
           (plan (plan-select-query statement)))
      (unwind-protect
           (call-with-admitted-select-plan
            plan
            (lambda (relations)
              (let ((text (format-query-plan plan relations))
                    (json (format-query-plan-json plan relations nil nil 0.0d0)))
                (rove:ok (equal (sql-json-input json) json))
                (dolist (label '("InitPlan 1 (scalar)" "CTE q (shared)"))
                  (rove:ok (search label text))
                  (rove:ok (search (format nil "\"Subplan Name\":~s" label) json))))))
        (reset-backend-transaction *backend*))))

(rove:deftest test-explain-filtered-rows
    (let* ((*backend* (make-backend))
           (ast (first (parse-statements
                        "select x from (select 1 as x union all select 2
                                      union all select 3 union all select null::int) v where x > 2")))
           (plan (plan-select-query ast))
           (*query-plan-instrumentation-p* :rows))
      (unwind-protect
           (call-with-admitted-select-plan
            plan
            (lambda (relations)
              (dotimes (run 2)
                (map-query-plan-rows plan relations (lambda (row) (declare (ignore row))))
                (let ((text (format-query-plan plan relations t))
                      (json (format-query-plan-json plan relations t nil 0.0d0)))
                  (rove:ok (search "Rows Removed by Filter: 3" text))
                  (rove:ok (equal (sql-json-input json) json))
                  (rove:ok (search "\"Rows Removed by Filter\":3" json))))))
        (reset-backend-transaction *backend*))))

(rove:deftest test-explain-join-filtered-rows
    (let ((*backend* (make-backend))
          (*query-plan-instrumentation-p* :rows))
      (unwind-protect
           (loop for (sql rejected) in
                 '(("select a.x from (select 1 x union all select 2) a
                   left join (select 1 x union all select 3 union all select null::int) b
                   on a.x = b.x" 5)
                   ("select a.x from (select 1 x union all select 2) a
                   left join lateral (select a.x + 1 y) b on a.x = b.y" 2))
                 for plan = (plan-select-query (first (parse-statements sql)))
                 do (call-with-admitted-select-plan
                     plan
                     (lambda (relations)
                       (let ((count 0))
                         (map-query-plan-rows plan relations (lambda (row)
                                                               (declare (ignore row))
                                                               (incf count)))
                         (rove:ok (= count 2))
                         (rove:ok (search (format nil "Rows Removed by Join Filter: ~d" rejected)
                                          (format-query-plan plan relations t)))
                         (rove:ok (search (format nil "\"Rows Removed by Join Filter\":~d" rejected)
                                          (format-query-plan-json plan relations t nil 0.0d0)))))))
        (reset-backend-transaction *backend*))))

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
    (map-query-plan-rows plan nil (lambda (row) (declare (ignore row))))
    (rove:ok (zerop (plan-node-metadata-actual-loops
                     (gethash aggregate-node (query-plan-metadata plan))))
             "ordinary execution leaves EXPLAIN ANALYZE counters untouched")
    (let ((*query-plan-instrumentation-p* t))
      (map-query-plan-rows plan nil
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
      (map-query-plan-rows plan nil (lambda (row) (declare (ignore row)))))
    (rove:ok (= (plan-node-metadata-actual-loops
                 (gethash aggregate-node (query-plan-metadata plan)))
                1)
             "each instrumented execution resets actual loop counters"))
  (let* ((plan (plan-select-query
                (first (parse-statements "select 7, count(*) where 1 = 0"))))
         (rows '()))
    (map-query-plan-rows plan nil (lambda (row) (push (car row) rows)))
    (rove:ok (equalp (nreverse rows) (list #(7 0)))
             "global COUNT(*) emits one zero row for empty input and permits constants"))
  (let* ((plan (plan-select-query
                (first (parse-statements "select true, count(*) where 1 = 0"))))
         (rows '()))
    (map-query-plan-rows plan nil (lambda (row) (push (car row) rows)))
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
      (let ((iterator
              (make-lateral-join-iterator
               (list :type :left :qual '(:bool t) :left-width 1 :right-width 1)
               (make-rove-row-iterator (list #(10) #(20)))
               (lambda (outer origins presence)
                 (declare (ignore origins presence))
                 (make-rove-row-iterator (when (= (aref outer 0) 10) (list #(100))))))))
        (unwind-protect (map-sql-iterator iterator (lambda (row) (push row rows)))
          (close-sql-iterator iterator)))
      (rove:ok (equalp (nreverse rows) (list #(10 100) (vector 20 +null+)))
               "parameterized LEFT join emits matches and null-extends misses"))
  (rove:ok (equalp
            (combine-select-join-environment
             #(10 +null+ 30) #(20)
             (list :reordered t :right-offset 1))
            #(10 20 30))
           "greedy join execution writes rows into source-order Var slots"))
