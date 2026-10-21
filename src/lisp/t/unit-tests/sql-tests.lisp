;;;; sql-tests.lisp -- SQL evaluation and error cleanup tests.

(in-package #:pg)

(defun test-sql-strict-let-evaluation ()
  (let ((evaluations '()))
    (check (eq +null+
               (sql-strict-let ((left (progn (push :left evaluations) +null+))
                                (right (progn (push :right evaluations) 7)))
                 :body-must-not-run))
           "strict let returns NULL when any binding is NULL")
    (check (equal (nreverse evaluations) '(:left :right))
           "strict let evaluates every binding once before NULL check"))
  (let ((evaluations 0))
    (check (= 9
              (sql-strict-let ((left (progn (incf evaluations) 4))
                               (right (progn (incf evaluations) 5)))
                (+ left right)))
           "strict let returns body result for non-NULL bindings")
    (check (= evaluations 2) "strict let bindings run once each")))

(defun test-insert-index-catalog-snapshot-lock ()
  (let ((original-table-indexes (symbol-function 'table-indexes))
        (first-index (make-pg-index :relfilenode 91001))
        (second-index (make-pg-index :relfilenode 91002))
        (lock-held nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'table-indexes)
                 (lambda (table)
                   (declare (ignore table))
                   (setf lock-held (sb-thread:holding-mutex-p *write-lock*))
                   (list first-index second-index)))
           (multiple-value-bind (indexes relfilenodes)
               (capture-insert-indexes nil)
             (check lock-held
                    "INSERT snapshots the shared index catalog under write lock")
             (check (equal indexes (list first-index second-index))
                    "INSERT snapshot preserves the table's index objects")
             (check (equal relfilenodes '(91001 91002))
                    "INSERT snapshot preserves index relation file nodes")))
      (setf (symbol-function 'table-indexes) original-table-indexes))))

(defun test-select-top-n-collector ()
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
      (check (= (array-dimension (select-top-n-collector-heap large-bound) 0) 32)
             "ordered LIMIT does not preallocate its full row bound"))
    (dolist (entry input)
      (offer-select-top-n-row
       collector (cons (vector (cdr entry)) (vector (car entry))))
      (check (<= (length (select-top-n-collector-heap collector)) 3)
             "ordered LIMIT retains no more than its row bound"))
    (check (equal (mapcar (lambda (row) (aref (car row) 0))
                          (select-top-n-collector-rows collector))
                  '(:zero :one-a :one-b))
           "ordered LIMIT returns the best rows and preserves ties in input order"))
  (let* ((order (list (cons '(:col 1) nil)))
         (collector (make-select-top-n-collector 40 order)))
    (loop for value from 40 downto 1
          do (offer-select-top-n-row collector
                                     (cons (vector value) (vector value))))
    (check (= (length (select-top-n-collector-heap collector)) 40)
           "ordered LIMIT grows its heap as candidates arrive")
    (check (<= (array-dimension (select-top-n-collector-heap collector) 0) 40)
           "ordered LIMIT caps heap capacity at its row bound")
    (check (equal (mapcar (lambda (row) (aref (car row) 0))
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
    (check (equal (mapcar (lambda (row) (aref (car row) 0))
                          (select-top-n-collector-rows collector))
                  '(:one-20-a :one-20-b :one-10))
           "ordered LIMIT supports mixed directions across multiple keys"))
  (let* ((order (list (cons '(:col 1) t)))
         (collector (make-select-top-n-collector 1 order)))
    (dolist (entry (list (cons 1 :one) (cons +null+ :null) (cons 2 :two)))
      (offer-select-top-n-row
       collector (cons (vector (cdr entry)) (vector (car entry)))))
    (check (equal (mapcar (lambda (row) (aref (car row) 0))
                          (select-top-n-collector-rows collector))
                  '(:null))
           "ordered LIMIT follows DESC NULL ordering")))

(defun test-using-join-output-metadata ()
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
    (check (= -1 (select-column-typmod right-output))
           "RIGHT USING reports the common typmod for differing inputs")
    (check (= -1 (result-column-typmod right-result))
           "RIGHT USING protocol metadata carries the common typmod")
    (check (= -1 (result-column-typmod right-explicit-result))
           "explicit RIGHT USING target carries the common typmod")
    (check (= 0 (getf right-explicit-target :table-oid))
           "relabelled RIGHT USING target has no base-column origin")
    (check (= 22 (getf same-explicit-target :table-oid))
           "unrelabelled RIGHT USING target retains its base-column origin")
    (check (= -1 (result-column-typmod full-result))
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
      (check (eq :int8 (getf explicit-int-target :type))
             "explicit USING target is promoted to the common integer type")
      (check (= +int8oid+ (result-column-type-oid int-result))
             "promoted USING protocol metadata reports bigint"))))

(defun test-error-cleanup-and-fatal-rethrow ()
  (let* ((fatal (make-condition 'pg-error :severity "FATAL"
                                          :sqlstate "XX000" :message "test fatal"))
         (cleanup-count 0)
         (caught nil))
    (handler-case
        (with-error-cleanup (condition (incf cleanup-count))
          (error fatal))
      (pg-error (condition)
        (setf caught condition)))
    (check (= cleanup-count 1) "error cleanup runs before rethrow")
    (check (eq caught fatal) "error cleanup re-signals the original condition"))
  (let* ((original (make-condition 'pg-error :severity "ERROR"
                                             :sqlstate "XX000"
                                             :message "original error"))
         (caught nil))
    (handler-case
        (with-error-cleanup (condition (error "injected cleanup failure"))
          (error original))
      (pg-error (condition) (setf caught condition)))
    (check (eq caught original)
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
    (check (not handler-ran) "nonfatal handler skips FATAL errors")
    (check (eq caught fatal) "nonfatal handler re-signals the original FATAL"))
  (check (eq :handled
             (handler-case-nonfatal
              (error "test nonfatal")
              (condition
               (if (typep condition 'error) :handled :unexpected))))
         "nonfatal handler handles ordinary errors"))

(defun test-create-table-cleanup-after-fsync-error ()
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend))
         (db (make-database :name "postgres" :oid 1))
         (original-fsync-index (symbol-function 'fsync-index))
         (caught nil))
    (setf (backend-database *backend*) "postgres"
          (gethash "postgres" (catalog-databases *catalog*)) db)
    (unwind-protect
         (progn
           (setf (symbol-function 'fsync-index)
                 (lambda (index)
                   (declare (ignore index))
                   (error "injected final index fsync failure")))
           (handler-case
               (exec-create-table
                nil
                '(:name "cleanup_test"
                  :columns ((:name "value" :type :int4 :typmod -1 :unique t))))
             (error (condition)
               (setf caught condition)))
           (check (typep caught 'error)
                  "CREATE TABLE re-signals final index fsync failure")
           (check (null (gethash "cleanup_test" (database-tables db)))
                  "CREATE TABLE cleanup removes table after final fsync failure"))
      (setf (symbol-function 'fsync-index) original-fsync-index))))

(defun test-query-plan-ir-and-count-aggregate ()
  (let* ((ast (first (parse-statements
                      "select category, count(*) as n from events group by category order by n desc limit 2")))
         (targets (getf ast :targets)))
    (check (string= (second (first (getf ast :group-by))) "category")
           "SELECT parser retains GROUP BY expressions")
    (check (eq (first (getf (second targets) :expr)) :count-star)
           "SELECT parser represents COUNT(*) as an aggregate node")
    (check (= (length (parse-statements "select *, count(*) from events")) 1)
           "SELECT parser accepts a star mixed with other targets")
    (let ((caught nil))
      (handler-case
          (parse-statements "select count(*) from events having count(*) > 1")
        (pg-error (condition) (setf caught condition)))
      (check (and caught (string= (pg-error-sqlstate caught) "0A000"))
             "HAVING remains explicitly unsupported"))
    (let ((plain (first (parse-statements "explain select 1")))
          (analyze (first (parse-statements "explain analyze select 1")))
          (parenthesized
            (first (parse-statements "explain (analyze) select 1"))))
      (check (not (getf plain :analyze-p))
             "EXPLAIN keeps ordinary plans non-executing")
      (check (getf analyze :analyze-p)
             "EXPLAIN ANALYZE is retained in the statement IR")
      (check (getf parenthesized :analyze-p)
             "EXPLAIN (ANALYZE) is accepted")))
  (let* ((plan (plan-select-query
                (first (parse-statements "select count(*)"))))
         (logical (query-plan-logical plan))
         (physical (query-plan-physical plan))
         (rows '())
         (aggregate-node
           (find :hash-aggregate (collect-query-plan-nodes physical)
                 :key #'first)))
    (check (eq (first (third logical)) :aggregate)
           "logical plan uses an abstract aggregate operator")
    (check (eq (first (third physical)) :hash-aggregate)
           "physical plan selects HashAggregate")
    (execute-query-plan plan nil (lambda (row) (declare (ignore row))))
    (check (zerop (plan-node-metadata-actual-loops
                   (gethash aggregate-node (query-plan-metadata plan))))
           "ordinary execution leaves EXPLAIN ANALYZE counters untouched")
    (let ((*query-plan-instrumentation-p* t))
      (execute-query-plan plan nil
                          (lambda (row) (push (aref (car row) 0) rows))))
    (check (equal (nreverse rows) '(1))
           "interpreter executes a plain COUNT(*) over its input")
    (check (= (plan-node-metadata-actual-rows
               (gethash aggregate-node (query-plan-metadata plan)))
              1)
           "operator sidecar records actual aggregate output rows")
    (let ((metadata (gethash aggregate-node (query-plan-metadata plan))))
      (check (= (plan-node-metadata-actual-loops metadata) 1)
             "EXPLAIN ANALYZE records one aggregate loop")
      (check (and (>= (plan-node-metadata-actual-startup-time-ticks metadata) 0)
                  (>= (plan-node-metadata-actual-total-time-ticks metadata) 0))
             "EXPLAIN ANALYZE records nonnegative aggregate timings")
      (let ((formatted (format-query-plan plan nil t 1.25d0)))
        (check (and (search "(actual time=" formatted)
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
      (check (and source-node root-metadata source-metadata)
             "instrumented aggregate plan has at least three measured levels")
      (check (>= (plan-node-metadata-actual-total-time-ticks root-metadata)
                 (plan-node-metadata-actual-total-time-ticks aggregate-metadata))
             "EXPLAIN ANALYZE parent timing includes its child exactly once")
      (check (>= (plan-node-metadata-actual-total-time-ticks aggregate-metadata)
                 (plan-node-metadata-actual-total-time-ticks source-metadata))
             "EXPLAIN ANALYZE aggregate timing includes its input exactly once"))
    (let ((*query-plan-instrumentation-p* t))
      (execute-query-plan plan nil (lambda (row) (declare (ignore row)))))
    (check (= (plan-node-metadata-actual-loops
               (gethash aggregate-node (query-plan-metadata plan)))
              1)
           "each instrumented execution resets actual loop counters"))
  (let* ((plan (plan-select-query
                (first (parse-statements "select 7, count(*) where 1 = 0"))))
         (rows '()))
    (execute-query-plan plan nil (lambda (row) (push (car row) rows)))
    (check (equalp (nreverse rows) (list #(7 0)))
           "global COUNT(*) emits one zero row for empty input and permits constants"))
  (let* ((plan (plan-select-query
                (first (parse-statements "select true, count(*) where 1 = 0"))))
         (rows '()))
    (execute-query-plan plan nil (lambda (row) (push (car row) rows)))
    (check (equalp (nreverse rows) (list #(t 0)))
           "global COUNT(*) permits row-independent boolean projections"))
  (let* ((group-ref (aggregate-output-ref 1 :int4))
         (targets (list (list :name "category" :expr group-ref)
                        (list :name "category" :expr (copy-tree group-ref))))
         (order (analyze-aggregate-order
                 (list (list :expr '(:column "category") :desc nil))
                 nil targets nil)))
    (check (equal (caar order) group-ref)
           "ORDER BY accepts duplicate output names for the same expression")))

(defun test-index-scan-limit-stops-duplicate-candidates ()
  (let* ((table (make-pg-class :oid 91011 :name "limit_index_test"
                               :relfilenode 91012 :columns #()))
         (index (make-pg-index :oid 91013 :relfilenode 91014
                               :table-oid 91011 :columns '(1)))
         (path (make-select-scan-path :kind :index-scan :index index
                                      :qual '(:bool t :type :bool)
                                      :key #(10)))
         (scan-node (list :index-scan 0 91011 91013
                          '(:bool t :type :bool) #(10)))
         (target (list :expr '(:col 1 :type :int4 :typmod -1)
                       :name "k" :type :int4 :typmod -1
                       :table-oid 0 :attnum 0))
         (plan (%make-query-plan
                :physical (list :limit 1
                                (list :project (list target)
                                      (list :filter '(:bool t :type :bool)
                                            scan-node)))))
         (registered (list (list :table table :path path)))
         (original-search (symbol-function 'btree-search))
         (original-get-tuple (symbol-function 'heapam-get-tuple))
         (original-visible (symbol-function 'htup-visible-p))
         (seen 0))
    (unwind-protect
         (progn
           (setf (symbol-function 'btree-search)
                 (lambda (relfilenode key)
                   (declare (ignore relfilenode key))
                   '((0 . 1) (0 . 2) (0 . 3)))
                 (symbol-function 'heapam-get-tuple)
                 (lambda (relfilenode block offnum columns)
                   (declare (ignore relfilenode block columns))
                   (make-htup :values (vector offnum)))
                 (symbol-function 'htup-visible-p)
                 (lambda (tuple snapshot)
                   (declare (ignore tuple snapshot))
                   t))
           (interpret-query-plan
            plan registered
            (lambda (row) (declare (ignore row)) (incf seen)))
           (check (= seen 1)
                  "LIMIT stops an index scan after its first duplicate-key candidate"))
      (setf (symbol-function 'btree-search) original-search
            (symbol-function 'heapam-get-tuple) original-get-tuple
            (symbol-function 'htup-visible-p) original-visible))
    (let* ((logical-scan
             (select-scan-plan-form 0 (list :table table) path nil))
           (physical-scan
             (select-scan-plan-form 0 (list :table table) path t))
           (metadata (make-plan-node-metadata
                      :selection-rule :equality-index))
           (plan (%make-query-plan
                  :physical (list :project nil
                                  (list :filter '(:bool t :type :bool)
                                        physical-scan)))))
      (setf (gethash physical-scan (query-plan-metadata plan)) metadata)
      (check (eq (first logical-scan) :scan)
             "logical plans keep the access method abstract")
      (check (eq (first physical-scan) :index-scan)
             "physical plans expose the selected IndexScan")
      (let* ((wrong-table (make-pg-class :oid 91015 :name "other_table"
                                         :relfilenode 91016 :columns #()))
             (caught nil))
        (handler-case
            (physical-scan-relation
             physical-scan (list (list :table wrong-table)))
          (error (condition) (setf caught condition)))
        (check caught
               "physical scans reject a registered table with a mismatched OID"))
      (let* ((wrong-index (make-pg-index :oid 91013 :relfilenode 91017
                                         :table-oid 91015 :columns '(1)))
             (wrong-path (make-select-scan-path
                          :kind :index-scan :index wrong-index
                          :qual '(:bool t :type :bool) :key #(10)))
             (index-node (select-scan-plan-form
                          0 (list :table table) path t))
             (caught nil))
        (handler-case
            (physical-scan-path
             index-node (list :table table :path wrong-path))
          (error (condition) (setf caught condition)))
        (check caught
               "physical IndexScan rejects an index owned by another table")
        (setf caught nil)
        (handler-case
            (format-query-plan
             (%make-query-plan :physical index-node)
             (list (list :table table :path wrong-path)))
          (error (condition) (setf caught condition)))
        (check caught
               "EXPLAIN formatting rejects an index path inconsistent with its node"))
      (reconcile-query-plan-access-paths
       plan (list (list :table table
                        :path (make-select-scan-path :kind :seq-scan))))
      (check (eq (first physical-scan) :seq-scan)
             "index replacement rewrites the executable plan to SeqScan")
      (check (eq (plan-node-metadata-selection-rule metadata)
                 :sequential-fallback)
             "plan metadata records a stale-index fallback"))))

(defun test-query-plan-compiler-backend-hook ()
  (let ((original (query-plan-backend-entry :unit-test-compiler))
        (compiled 0)
        (rows '())
        (plan (plan-select-query
               (first (parse-statements "select 42")))))
    (unwind-protect
         (progn
           (register-query-plan-backend
            :unit-test-compiler
            :compiler (lambda (received-plan)
                        (check (eq received-plan plan)
                               "compiler receives the physical query plan")
                        (incf compiled)
                        (lambda (relations callback)
                          (declare (ignore relations))
                          (interpret-query-plan received-plan nil callback))))
           (execute-query-plan
            plan nil (lambda (row) (push (aref (car row) 0) rows))
            :backend :unit-test-compiler)
           (check (= compiled 1) "compiler backend hook is invoked once")
           (check (equal rows '(42))
                  "compiled backend can route execution through an alternate function"))
      (sb-thread:with-mutex (*query-plan-backend-lock*)
        (if original
            (setf (gethash :unit-test-compiler *query-plan-backends*) original)
            (remhash :unit-test-compiler *query-plan-backends*))))))

(defun test-query-plan-compiler-dispatch-uses-registry-snapshot ()
  (let* ((name :unit-test-compiler-snapshot)
         (original (query-plan-backend-entry name))
         (original-entry-reader (symbol-function 'query-plan-backend-entry))
         (plan (plan-select-query
                (first (parse-statements "select 42"))))
         (rows '())
         (lookups 0)
         (caught nil)
         (replacement
           (lambda (received-plan relations callback)
             (declare (ignore received-plan relations))
             (funcall callback (cons #(99) nil)))))
    (unwind-protect
         (progn
           (register-query-plan-backend
            name
            :compiler (lambda (received-plan)
                        (declare (ignore received-plan))
                        (lambda (relations callback)
                          (declare (ignore relations))
                          (funcall callback (cons #(42) nil)))))
           (setf (symbol-function 'query-plan-backend-entry)
                 (lambda (backend)
                   (if (eq backend name)
                       (progn
                         (incf lookups)
                         (if (= lookups 1)
                             (prog1 (funcall original-entry-reader backend)
                               ;; Model replacement after dispatch captured the
                               ;; compiler entry but before a second lookup.
                               (register-query-plan-backend
                                name :executor replacement))
                             (funcall original-entry-reader backend)))
                       (funcall original-entry-reader backend))))
           (handler-case
               (execute-query-plan
                plan nil (lambda (row) (push (aref (car row) 0) rows))
                :backend name)
             (error (condition) (setf caught condition)))
           (check (null caught)
                  "compiler dispatch keeps the backend entry captured at lookup")
           (check (= lookups 1)
                  "compiler dispatch reads the backend registry only once")
           (check (equal rows '(42))
                  "a concurrent backend replacement does not change active dispatch"))
      (setf (symbol-function 'query-plan-backend-entry) original-entry-reader)
      (sb-thread:with-mutex (*query-plan-backend-lock*)
        (if original
            (setf (gethash name *query-plan-backends*) original)
            (remhash name *query-plan-backends*))))))

(defun test-query-plan-backend-registry-synchronization ()
  (let* ((name :unit-test-backend-registry)
         (original (query-plan-backend-entry name))
         (plan (plan-select-query
                (first (parse-statements "select 42"))))
         (failures '())
         (failure-lock (sb-thread:make-mutex :name "backend registry test")))
    (labels ((executor-for (value)
               (lambda (received-plan relations callback)
                 (declare (ignore received-plan relations))
                 (funcall callback
                          (cons (vector value
                                        (sb-thread:holding-mutex-p
                                         *query-plan-backend-lock*))
                                nil))))
             (record-failure (failure)
               (sb-thread:with-mutex (failure-lock)
                 (push failure failures))))
      (unwind-protect
           (progn
             (register-query-plan-backend name :executor (executor-for 1))
             (let ((writer
                     (sb-thread:make-thread
                      (lambda ()
                        (dotimes (iteration 300)
                          (register-query-plan-backend
                           name :executor (executor-for
                                           (if (evenp iteration) 1 2)))))))
                   (reader
                     (sb-thread:make-thread
                      (lambda ()
                        (loop repeat 300
                              do
                                 (handler-case
                                     (let ((row nil))
                                       (execute-query-plan
                                        plan nil (lambda (result) (setf row result))
                                        :backend name)
                                       (unless (and row
                                                    (member (aref (car row) 0) '(1 2))
                                                    (not (aref (car row) 1)))
                                         (record-failure row)))
                                   (error (condition)
                                     (record-failure condition))))))))
               (sb-thread:join-thread writer)
               (sb-thread:join-thread reader))
             (check (null failures)
                    "concurrent backend registration and dispatch use synchronized snapshots"))
        (sb-thread:with-mutex (*query-plan-backend-lock*)
          (if original
              (setf (gethash name *query-plan-backends*) original)
              (remhash name *query-plan-backends*)))))))

(defun test-relation-build-own-xid ()
  (let ((relfilenode 987653)
        (xid 987654))
    (setf (gethash relfilenode *relation-writers*) (list xid))
    (let ((token (acquire-relation-build-lock relfilenode xid)))
      (check (eq token (gethash relfilenode *relation-builders*))
             "relation build ignores its own writer XID")
      (release-relation-build-lock relfilenode token))
    (remhash relfilenode *relation-writers*)))

(defun test-parse-error-aborts-transaction ()
  (let ((path (merge-pathnames "parse-error.out" *pgdata*)))
    (setf (backend-state *backend*) :in-progress
          (backend-explicit *backend*) t
          (backend-command-id *backend*) 0)
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "select from;"))
    (check (eq (backend-state *backend*) :aborted)
           "a parse error aborts an explicit transaction")
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "select 1;"))
    (check (eq (backend-state *backend*) :aborted)
           "aborted transaction stays aborted")
    (abort-transaction)
    (check (eq (backend-state *backend*) :idle)
           "abort clears the transaction state")))
