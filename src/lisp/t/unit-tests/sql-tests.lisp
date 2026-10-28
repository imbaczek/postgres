;;;; sql-tests.lisp -- SQL evaluation and error cleanup tests.

(in-package #:pg)

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

(defun test-replication-slots-catalog-view ()
  (let* ((*backend* (make-backend))
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (*wal-durable-lsn* 4096)
         (slot (make-wal-replication-slot
                :name "view_slot" :restart-lsn 1024 :consistent-lsn 2048
                :confirmed-flush-lsn 3072 :active-owner (gensym "OWNER-")
                :wal-status :reserved))
         (ast (first (parse-statements
                      "SELECT * FROM pg_catalog.pg_replication_slots")))
         (plan (plan-select-query ast))
         (relations (getf (query-plan-prepared plan) :relations))
         (rows '()))
    (setf (gethash "view_slot" *wal-replication-slots*) slot)
    (with-registered-select-relations (registered relations)
      (execute-query-plan plan registered
                          (lambda (row) (push row rows))))
    (setf rows (nreverse rows))
    (check (= (length rows) 1)
           "pg_catalog.pg_replication_slots returns live slot rows")
    (when rows
      (let ((values (caar rows)))
        (check (equal (aref values 0) "view_slot")
               "replication-slot view exposes the slot name")
        (check (eq (aref values 1) t)
               "replication-slot view exposes active state")
        (check (= (aref values 2) 1024)
               "replication-slot view stores restart LSN numerically")
        (check (= (aref values 3) 3072)
               "replication-slot view stores confirmed flush LSN numerically")
        (check (equal (value->text (aref values 2) :pg-lsn) "0/00000400")
               "replication-slot view formats restart LSN")
        (check (equal (value->text (aref values 3) :pg-lsn) "0/00000C00")
               "replication-slot view formats confirmed flush LSN")
        (check (= (aref values 5) 3072)
               "replication-slot view reports retained WAL bytes")
        (check (equal (aref values 4) "reserved")
               "replication-slot view reports WAL status")))))

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

(defun test-analyze-parser-and-costed-join-order ()
  (check (equal (first (parse-statements "ANALYZE events"))
                '(:type :analyze :table "events"))
         "ANALYZE parses as a table statistics command")
  (check (and (catalog-change-p (first (parse-statements "ANALYZE events")))
              (not (ddl-p (first (parse-statements "ANALYZE events")))))
         "ANALYZE is non-transactional catalog work, not schema DDL")
  (let* ((column-a (make-pg-column :name "id" :type :int4 :typmod -1
                                   :attnum 1 :notnull nil))
         (column-b (make-pg-column :name "id" :type :int4 :typmod -1
                                   :attnum 1 :notnull nil))
         (table-a (make-pg-class
                   :oid 93001 :name "a" :relfilenode 93011
                   :columns (vector column-a) :indexes '(93021)
                   :statistics
                   (make-pg-table-statistics
                    :row-count 100000 :sample-count 1
                    :columns (vector (make-pg-column-statistics
                                      :null-fraction 0.0d0
                                      :distinct-count 100000
                                      :most-common-values nil)))))
         (table-b (make-pg-class
                   :oid 93002 :name "b" :relfilenode 93012
                   :columns (vector column-b) :indexes nil
                   :statistics
                   (make-pg-table-statistics
                    :row-count 100 :sample-count 1
                    :columns (vector (make-pg-column-statistics
                                      :null-fraction 0.0d0
                                      :distinct-count 100
                                      :most-common-values nil)))))
         (index-a (make-pg-index :oid 93021 :name "a_id_idx"
                                 :relfilenode 93031 :table-oid 93001
                                 :columns '(1) :valid-p t))
         (original-resolver (symbol-function 'select-table-or-error))
         (original-indexes (symbol-function 'table-indexes))
         (original-buf-nblocks (symbol-function 'buf-nblocks)))
    (unwind-protect
         (progn
           (setf (symbol-function 'select-table-or-error)
                 (lambda (name)
                   (cond ((string= name "a") table-a)
                         ((string= name "b") table-b)
                         (t (error "unexpected test relation ~a" name))))
                 (symbol-function 'table-indexes)
                 (lambda (table)
                   (if (eq table table-a) (list index-a) nil))
                 (symbol-function 'buf-nblocks)
                 (lambda (relfilenode)
                   (case relfilenode
                     (93011 1000) (93012 10) (93031 2) (t 1))))
           (let* ((plan (plan-select-query
                         (first (parse-statements
                                 "SELECT a.id FROM a JOIN b ON a.id = b.id"))))
                  (prepared (query-plan-prepared plan))
                  (relations (getf prepared :relations))
                  (path-a (getf (first relations) :path))
                  (path-b (getf (second relations) :path)))
             (check (equal (getf prepared :join-order) '(1 0))
                    "greedy INNER join planning starts with the smaller relation")
             (check (eq (select-scan-path-kind path-b) :seq-scan)
                    "cheapest base input uses a sequential scan")
             (check (and (eq (select-scan-path-kind path-a) :index-scan)
                         (equal (select-scan-path-parameter-expr path-a)
                                '(:col 2 :type :int4 :typmod -1 :notnull nil
                                  :table-oid 93002 :attnum 1)))
                    "join planner chooses an indexed inner path keyed by outer Var")
             (check (find-if (lambda (node)
                               (and (eq (first node) :index-scan)
                                    (= (second node) 0)
                                    (seventh node)))
                             (collect-query-plan-nodes
                              (query-plan-physical plan)))
                    "physical plan retains the parameter expression for execution")))
      (setf (symbol-function 'select-table-or-error) original-resolver
            (symbol-function 'table-indexes) original-indexes
            (symbol-function 'buf-nblocks) original-buf-nblocks))))

(defun test-query-plan-index-lock-order ()
  (let* ((table-high (make-pg-class :oid 93011 :name "high" :relfilenode 93011
                                    :columns #()))
         (table-low (make-pg-class :oid 93012 :name "low" :relfilenode 93012
                                   :columns #()))
         (index-high (make-pg-index :oid 93021 :relfilenode 202
                                    :table-oid 93011 :columns '(1)))
         (index-low (make-pg-index :oid 93022 :relfilenode 101
                                   :table-oid 93012 :columns '(1)))
         (relations
           (list (list :table table-high
                       :path (make-select-scan-path :kind :index-scan
                                                    :index index-high))
                 (list :table table-low
                       :path (make-select-scan-path :kind :index-scan
                                                    :index index-low))))
         (plan
           (%make-query-plan
            :physical
            (list :nested-loop-join
                  (list :type :inner)
                  (list :index-scan 0 93011 93021 '(:bool t :type :bool) #(1))
                  (list :index-scan 1 93012 93022 '(:bool t :type :bool) #(1)))))
         (original-ensure (symbol-function 'btree-ensure-init-before-read))
         (original-locks (symbol-function 'call-with-btree-relation-read-leases))
         (original-backend-entry (symbol-function 'query-plan-backend-entry))
         (locked-relfilenodes nil)
         (executor-ran-under-locks-p nil))
    (unwind-protect
         (progn
           (check (equal (query-plan-index-relfilenodes plan relations) '(101 202))
                  "nested IndexScans resolve to sorted, deduplicated relation IDs")
           (setf (symbol-function 'btree-ensure-init-before-read)
                 (lambda (relfilenode) (declare (ignore relfilenode)) (values))
                 (symbol-function 'call-with-btree-relation-read-leases)
                 (lambda (relfilenodes function)
                   (setf locked-relfilenodes relfilenodes)
                   (funcall original-locks relfilenodes function))
                 (symbol-function 'query-plan-backend-entry)
                 (lambda (backend)
                   (declare (ignore backend))
                   (make-query-plan-backend
                    :executor
                    (lambda (query registered callback)
                      (declare (ignore query registered callback))
                      (setf executor-ran-under-locks-p
                            (equal locked-relfilenodes '(101 202)))))))
           (execute-query-plan plan relations nil :backend :lock-order-test)
           (check executor-ran-under-locks-p
                  "executor starts only after the full sorted index read-lock set"))
      (setf (symbol-function 'btree-ensure-init-before-read) original-ensure
            (symbol-function 'call-with-btree-relation-read-leases) original-locks
            (symbol-function 'query-plan-backend-entry) original-backend-entry))))

(defun test-select-toast-reader-domain ()
  (let* ((toast (make-pg-toast :oid 100 :relfilenode 104 :index-relfilenode 102))
         (table (make-pg-class :oid 101 :relfilenode 101 :columns #() :toast toast))
         (spec (list :table table :columns (pg-class-columns table)
                     :path (make-select-scan-path :kind :seq-scan)))
         (relations (list spec))
         (plan (%make-query-plan :physical '(:result)))
         (original-nblocks (symbol-function 'buf-nblocks))
         (original-ensure (symbol-function 'ensure-current-table)))
    (unwind-protect
         (progn
           (check (equal (select-relation-reader-relfilenodes relations) '(101 104 102))
                  "SELECT captures parent and owned TOAST relations")
           (setf (symbol-function 'buf-nblocks)
                 (lambda (node) (declare (ignore node)) 2))
           (check (equal (query-plan-index-relfilenodes plan relations) '(102))
                  "sequential SELECT includes the TOAST index in its lease domain")
           (setf (symbol-function 'ensure-current-table)
                 (lambda (table) (declare (ignore table)) t))
           (revalidate-select-relations relations)
           (setf (pg-toast-index-relfilenode toast) 103)
           (check (equal (getf spec :registered-toast-nodes) '(104 102))
                  "captured TOAST node identities survive catalog mutation")
           (check (handler-case (progn (revalidate-select-relations relations) nil)
                    (error () t))
                  "SELECT rejects a changed TOAST node")
           (check (handler-case (progn (revalidate-subquery-relations relations) nil)
                    (error () t))
                  "DML subqueries reject a changed TOAST node")
           (setf (symbol-function 'buf-nblocks)
                 (lambda (node) (declare (ignore node)) 1))
           (check (handler-case (progn (query-plan-index-relfilenodes plan relations) nil)
                    (error () t))
                  "missing TOAST index is rejected before initialization"))
      (setf (symbol-function 'buf-nblocks) original-nblocks
            (symbol-function 'ensure-current-table) original-ensure))))

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
         (original-btree-scan (symbol-function 'call-with-btree-equal-cursor))
         (original-next-tid (symbol-function 'btree-cursor-next-tid))
         (original-get-tuple (symbol-function 'heapam-get-tuple))
         (original-visible (symbol-function 'htup-visible-p))
         (scan-calls 0)
         (candidate-visits 0)
         (heap-visits 0)
         (seen 0))
    (unwind-protect
         (progn
           (setf (symbol-function 'call-with-btree-equal-cursor)
                 (lambda (relfilenode key function)
                   (incf scan-calls)
                   (call-with-btree-relation-read-lease
                    relfilenode
                    (lambda (lease)
                      (funcall function
                               (make-btree-equal-cursor
                                :relfilenode relfilenode :key key
                                :stack (list (make-btree-equal-cursor-frame
                                              :level 0 :end 2
                                              :entries (loop for offset from 1 to 3
                                                             collect (list key 0 offset)))))
                               lease))))
                 (symbol-function 'btree-cursor-next-tid)
                 (lambda (cursor)
                   (incf candidate-visits)
                   (funcall original-next-tid cursor))
                 (symbol-function 'heapam-get-tuple)
                 (lambda (relfilenode block offnum columns)
                   (declare (ignore relfilenode block columns))
                   (incf heap-visits)
                   (make-htup :values (vector offnum)))
                 (symbol-function 'htup-visible-p)
                 (lambda (tuple snapshot)
                   (declare (ignore tuple snapshot))
                   t))
           (interpret-query-plan
            plan registered
            (lambda (row) (declare (ignore row)) (incf seen)))
           (check (and (= seen 1) (= heap-visits 1)
                       (= candidate-visits 1) (= scan-calls 1))
                  "LIMIT stops a duplicate-key cursor after its first candidate"))
      (setf (symbol-function 'call-with-btree-equal-cursor) original-btree-scan
            (symbol-function 'btree-cursor-next-tid) original-next-tid
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
