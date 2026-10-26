;;;; catalog-tests.lisp -- catalog and index ordering tests.

(in-package #:pg)

(defun test-catalog-mutation-rollback-macro ()
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend :database "postgres"))
         (database (make-database :name "postgres" :oid 7))
         (table
           (make-pg-class
            :oid 701 :name "rollback_rows" :relfilenode 1701
            :columns
            (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                    :attnum 1 :notnull t))))
         (original-next-oid (catalog-next-oid *catalog*)))
    (setf (gethash "postgres" (catalog-databases *catalog*)) database
          (gethash "rollback_rows" (database-tables database)) table)
    (let ((failed nil))
      (handler-case
          (with-write-catalog-locks
            (with-catalog-mutation-rollback (database)
              (setf (pg-column-notnull (aref (pg-class-columns table) 0)) nil
                    (gethash "transient" (database-tables database))
                    (make-pg-class :oid 702 :name "transient" :columns #()))
              (incf (catalog-next-oid *catalog*))
              (error "injected pre-durable catalog failure")))
        (error () (setf failed t)))
      (check (and failed
                  (eq table (gethash "rollback_rows" (database-tables database)))
                  (not (gethash "transient" (database-tables database)))
                  (pg-column-notnull (aref (pg-class-columns table) 0))
                  (= (catalog-next-oid *catalog*) original-next-oid))
             "catalog mutation macro restores object state, catalog maps, and OID allocation"))
    (let ((failed nil))
      (handler-case
          (with-write-catalog-locks
            (with-catalog-mutation-rollback (database)
              (setf (pg-column-notnull (aref (pg-class-columns table) 0)) nil
                    *catalog-save-error-after-durable-p* t)
              (error "injected post-durable catalog failure")))
        (error () (setf failed t)))
      (check (and failed
                  (not (pg-column-notnull (aref (pg-class-columns table) 0))))
             "catalog mutation macro keeps state after the durable WAL boundary"))
    ;; Exercise the generation path through save-catalog itself, after its
    ;; catalog terminal has crossed the WAL boundary like RecordTransactionCommit()
    ;; in xact.c. This guards against a rollback after a durable catalog image.
    (logical-test-with-wal-storage
     (lambda ()
       (let* ((*catalog* (make-catalog))
              (*backend* (make-backend :database "postgres"))
              (*catalog-durable-generation* 0)
              (*wal-catalog-flush-generation* 0)
              (database (make-database :name "postgres" :oid 7))
              (table
                (make-pg-class
                 :oid 701 :name "rollback_rows" :relfilenode 1701
                 :columns
                 (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                         :attnum 1 :notnull t))))
              (event (logical-encode-catalog-event
                      7 '(:type :create-table :name "rollback_event")))
              (generation *catalog-durable-generation*)
              (caught nil))
         (setf (gethash "postgres" (catalog-databases *catalog*)) database
               (gethash "rollback_rows" (database-tables database)) table
               (gethash :catalog-rollback-test
                        *wal-logical-catalog-event-interests*) 7)
         (let ((*logical-test-failpoint-hook*
                 (lambda (point &rest details)
                   (declare (ignore details))
                   (when (eq point :after-catalog-event-wal-terminal)
                     (error "injected failure after durable catalog terminal")))))
           (handler-case
               (with-write-catalog-locks
                 (with-catalog-mutation-rollback (database)
                   ;; Rollback would restore NOT NULL, so a false result below
                   ;; proves the post-terminal mutation stayed published.
                   (setf (pg-column-notnull
                          (aref (pg-class-columns table) 0)) nil)
                   (save-catalog :logical-event event)))
             (error () (setf caught t))))
         (check (and caught
                     (> *catalog-durable-generation* generation)
                     (plusp *wal-durable-lsn*)
                     (not (pg-column-notnull (aref (pg-class-columns table) 0))))
                "catalog mutation macro keeps state after save-catalog advances the durable generation"))))))

(defun test-with-table-ddl-evaluates-lock-inputs-once ()
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend :database "postgres"))
         (*relation-builders* (make-hash-table :test #'eql))
         (*relation-reader-blockers* (make-hash-table :test #'eql))
         (*relation-build-groups* (make-hash-table :test #'eq))
         (database (make-database :name "postgres" :oid 7))
         (table (make-pg-class :oid 810 :name "macro_rows"
                               :relfilenode 810 :columns #()))
         (index (make-pg-index :oid 820 :name "macro_rows_idx"
                               :relfilenode 820 :table-oid 810))
         (table-evaluations 0)
         (index-evaluations 0)
         (wait-evaluations 0)
         (locked-relations nil)
         (reader-blocked-p nil)
         (body-ran nil))
    (setf (gethash "postgres" (catalog-databases *catalog*)) database
          (gethash "macro_rows" (database-tables database)) table)
    (let ((result
            (with-table-ddl
                ((progn (incf table-evaluations) table)
                 :indexes (progn (incf index-evaluations) (list index))
                 :wait-readers (progn (incf wait-evaluations) t))
              (maphash
               (lambda (token entry)
                 (declare (ignore token))
                 (setf locked-relations (first entry)
                       reader-blocked-p (second entry)))
               *relation-build-groups*)
              (setf body-ran t)
              table)))
      (check (eq result table)
             "table DDL macro returns its body result")
      (check (and (= table-evaluations 1)
                  (= index-evaluations 1)
                  (= wait-evaluations 1))
             "table DDL macro evaluates table, index, and wait forms once")
      (check (and (equal locked-relations '(810 820))
                  reader-blocked-p
                  body-ran)
             "table DDL macro locks its table and indexes before the body"))
    (let ((rejected nil)
          (stale-body-ran nil))
      (handler-case
          (with-table-ddl
              ((progn (remhash "macro_rows" (database-tables database)) table))
            (setf stale-body-ran t))
        (error () (setf rejected t)))
      (check (and rejected (not stale-body-ran))
             "table DDL macro revalidates a stale table before catalog mutation"))))

(defun test-analyze-column-statistics-and-costing ()
  (let* ((sample (vector #(1) #(1) (vector +null+) #(2) #(3) #(3)))
         (column (analyze-column-statistics sample 0 6))
         (table (make-pg-class
                 :oid 57 :name "analyzed" :relfilenode 5701 :columns #()
                 :statistics
                 (make-pg-table-statistics
                  :row-count 100000 :sample-count 6 :columns (vector column))))
         (seq (make-select-scan-path :kind :seq-scan))
         (index (make-pg-index :oid 58 :name "analyzed_idx"
                               :relfilenode 5702 :table-oid 57))
         (index-path (make-select-scan-path
                      :kind :index-scan :index index :selectivity 0.001d0))
         (original-buf-nblocks (symbol-function 'buf-nblocks)))
    (check (= (pg-column-statistics-distinct-count column) 3)
           "ANALYZE estimates distinct non-NULL sample values")
    (check (< (abs (- (pg-column-statistics-null-fraction column)
                      (/ 1.0d0 6)))
              1.0d-9)
           "ANALYZE records sampled NULL fraction")
    (check (= (cdr (assoc 1 (pg-column-statistics-most-common-values column)))
              (/ 2.0d0 6))
           "ANALYZE stores MCV frequencies over the full sample")
    (check (= (planner-equality-selectivity table 1 1) (/ 2.0d0 6))
           "planner uses an MCV frequency for equality selectivity")
    (unwind-protect
         (progn
           (setf (symbol-function 'buf-nblocks)
                 (lambda (relfilenode)
                   (case relfilenode (5701 1000) (5702 20) (t 1))))
           (check (eq (choose-cheapest-select-path table (list seq index-path))
                      index-path)
                  "cost model chooses a selective index scan over SeqScan"))
      (setf (symbol-function 'buf-nblocks) original-buf-nblocks))))

(defun test-analyze-retained-byte-vector-payload (sample)
  "Count payload bytes in the simple byte vectors reachable from SAMPLE."
  (loop for row across sample
        for value = (aref row 0)
        when (analyze-byte-vector-p value)
          sum (array-total-size value)))

(defun test-analyze-byte-vector-sample-retention ()
  (let* ((table (make-pg-class :oid 5902 :name "byte_sample"
                               :relfilenode 5902 :columns #()))
         (original-heapam-scan (symbol-function 'heapam-scan))
         (payload-size (* 128 1024)))
    (unwind-protect
         (progn
           (setf (symbol-function 'heapam-scan)
                 (lambda (relfilenode columns snapshot callback)
                   (declare (ignore relfilenode columns snapshot))
                   (dotimes (index 300)
                     (funcall callback
                              (make-htup
                               :values
                               (vector (make-array
                                        payload-size
                                        :element-type '(unsigned-byte 8)
                                        :initial-element (mod index 256))))))))
           (let ((*analyze-random-function*
                   (lambda (limit) (declare (ignore limit)) 0)))
             (multiple-value-bind (row-count sample)
                 (analyze-sample-visible-rows table 5902 nil)
               (let ((retained-bytes
                       (test-analyze-retained-byte-vector-payload sample)))
                 (check (= row-count 300)
                        "ANALYZE counts all rows containing large byte vectors")
                 (check (< (length sample) 300)
                        "ANALYZE reduces the sample for large byte-vector rows")
                 (check (= retained-bytes (* (length sample) payload-size))
                        "sample payload measurement reads retained vector storage")
                 (check (<= retained-bytes +analyze-sample-memory-bytes+)
                        "retained byte-vector payload stays within the 4 MiB budget")))))
      (setf (symbol-function 'heapam-scan) original-heapam-scan))))

(defun test-analyze-byte-vector-reservoir-shrink-and-oversize ()
  (let* ((table (make-pg-class :oid 5903 :name "byte_sample_edges"
                               :relfilenode 5903 :columns #()))
         (original-heapam-scan (symbol-function 'heapam-scan))
         (small-size 16)
         (large-size (* 64 1024)))
    (unwind-protect
         (progn
           (setf (symbol-function 'heapam-scan)
                 (lambda (relfilenode columns snapshot callback)
                   (declare (ignore relfilenode columns snapshot))
                   (dotimes (index 600)
                     (funcall callback
                              (make-htup
                               :values
                               (vector
                                (make-array
                                 (if (< index 300) small-size large-size)
                                 :element-type '(unsigned-byte 8)
                                 :initial-element (mod index 256))))))))
           (let ((*analyze-random-function*
                   (lambda (limit) (declare (ignore limit)) 0)))
             (multiple-value-bind (row-count sample)
                 (analyze-sample-visible-rows table 5903 nil)
               (check (= row-count 600)
                      "ANALYZE continues sampling after a large byte value shrinks capacity")
               (check (and (< (length sample) 300)
                           (some (lambda (row)
                                   (> (array-total-size (aref row 0)) small-size))
                                 sample))
                      "shrunk byte-vector reservoir considers later large values")
               (check (<= (test-analyze-retained-byte-vector-payload sample)
                          +analyze-sample-memory-bytes+)
                      "mixed-size retained byte payload stays within budget")))
           (setf (symbol-function 'heapam-scan)
                 (lambda (relfilenode columns snapshot callback)
                   (declare (ignore relfilenode columns snapshot))
                   (funcall callback
                            (make-htup
                             :values
                             (vector (make-array
                                      (1+ +analyze-sample-memory-bytes+)
                                      :element-type '(unsigned-byte 8)
                                      :initial-element 1))))
                   (funcall callback
                            (make-htup :values (vector (make-array
                                                        3
                                                        :element-type
                                                        '(unsigned-byte 8)
                                                        :initial-contents '(4 5 6)))))))
           (multiple-value-bind (row-count sample)
               (analyze-sample-visible-rows table 5903 nil)
             (check (= row-count 2)
                    "ANALYZE counts an oversized byte row before a small row")
             (check (and (= (length sample) 1)
                         (= (test-analyze-retained-byte-vector-payload sample) 3))
                    "ANALYZE skips an individually oversized byte row"))
           (setf (symbol-function 'heapam-scan)
                 (lambda (relfilenode columns snapshot callback)
                   (declare (ignore relfilenode columns snapshot))
                   (dotimes (index 2)
                     (funcall callback
                              (make-htup
                               :values
                               (vector (make-array
                                        (1+ +analyze-sample-memory-bytes+)
                                        :element-type '(unsigned-byte 8)
                                        :initial-element index)))))))
           (multiple-value-bind (row-count sample)
               (analyze-sample-visible-rows table 5903 nil)
             (check (and (= row-count 2) (zerop (length sample)))
                    "ANALYZE returns an empty sample when every byte row is oversized"))
           (setf (symbol-function 'heapam-scan)
                 (lambda (relfilenode columns snapshot callback)
                   (declare (ignore relfilenode columns snapshot callback))))
           (multiple-value-bind (row-count sample)
               (analyze-sample-visible-rows table 5903 nil)
             (check (and (zerop row-count) (zerop (length sample)))
                    "ANALYZE handles a relation with no visible rows")))
      (setf (symbol-function 'heapam-scan) original-heapam-scan))))

(defun test-analyze-reservoir-sample-is-bounded ()
  (let* ((table (make-pg-class
                 :oid 59 :name "sampled" :relfilenode 5901
                 :columns (vector (make-pg-column :name "id" :type :int4
                                                  :typmod -1 :attnum 1))))
         (original-heapam-scan (symbol-function 'heapam-scan)))
    (unwind-protect
         (progn
           (setf (symbol-function 'heapam-scan)
                 (lambda (relfilenode columns snapshot callback)
                   (declare (ignore relfilenode columns snapshot))
                   (dotimes (value 1000)
                     (funcall callback (make-htup :values (vector value))))))
           (multiple-value-bind (row-count sample)
               (analyze-sample-visible-rows table 5901 nil)
             (check (= row-count 1000)
                    "ANALYZE counts visible rows while sampling")
             (check (= (length sample) +analyze-sample-size+)
                    "ANALYZE reservoir remains at its configured sample bound")
             (check (every (lambda (row)
                             (and (integerp (aref row 0))
                                  (<= 0 (aref row 0) 999)))
                           sample)
                    "ANALYZE sample contains copied visible row values"))
           (let ((*analyze-random-function*
                   (lambda (limit) (declare (ignore limit)) 0))
                 (row (make-array 30000 :initial-element +null+)))
             (setf (symbol-function 'heapam-scan)
                   (lambda (relfilenode columns snapshot callback)
                     (declare (ignore relfilenode columns snapshot))
                     (dotimes (value 100)
                       (setf (aref row 0) value)
                       (funcall callback (make-htup :values row)))))
             (multiple-value-bind (row-count sample)
                 (analyze-sample-visible-rows table 5901 nil)
               (check (= row-count 100)
                      "ANALYZE counts rows after its byte-limited sample fills")
               (check (and (< (length sample) +analyze-sample-size+)
                           (= (aref (aref sample 0) 0) 99))
                      "byte-limited reservoir shrinks and still considers later rows")))
           (let ((*analyze-random-function*
                   (lambda (limit) (declare (ignore limit)) 0))
                 (wide-row (make-array 100000 :initial-element +null+)))
             (setf (aref wide-row 0) 999
                   (symbol-function 'heapam-scan)
                   (lambda (relfilenode columns snapshot callback)
                     (declare (ignore relfilenode columns snapshot))
                     (dotimes (value +analyze-sample-size+)
                       (funcall callback (make-htup :values (vector value))))
                     (funcall callback (make-htup :values wide-row))))
             (multiple-value-bind (row-count sample)
                 (analyze-sample-visible-rows table 5901 nil)
               (check (= row-count (1+ +analyze-sample-size+))
                      "ANALYZE counts rows when a wider row reduces reservoir capacity")
               (check (and (= (length sample) 5)
                           (= (length (aref sample 0)) 100000)
                           (= (aref (aref sample 0) 0) 999))
                      "ANALYZE shrinks an existing reservoir when wider rows arrive")))
           (setf (symbol-function 'heapam-scan)
                 (lambda (relfilenode columns snapshot callback)
                   (declare (ignore relfilenode columns snapshot))
                   (funcall callback
                            (make-htup
                             :values (make-array 600000
                                                 :initial-element +null+)))
                   (funcall callback (make-htup :values #(7)))))
           (multiple-value-bind (row-count sample)
               (analyze-sample-visible-rows table 5901 nil)
             (check (= row-count 2)
                    "ANALYZE counts rows even when one row exceeds sample bytes")
             (check (and (= (length sample) 1)
                         (= (aref (aref sample 0) 0) 7))
                    "ANALYZE skips oversized sample copies under its byte budget")))
      (setf (symbol-function 'heapam-scan) original-heapam-scan))))

(defun test-analyze-publish-restores-on-save-failure ()
  (let* ((table (make-pg-class :oid 60 :name "stats_failure"
                               :relfilenode 6001 :columns #()))
         (previous (make-pg-table-statistics :row-count 1))
         (candidate (make-pg-table-statistics :row-count 2))
         (original-save (symbol-function 'save-catalog))
         (original-ensure (symbol-function 'ensure-current-table))
         (caught nil))
    (setf (pg-class-statistics table) previous)
    (unwind-protect
         (progn
           (setf (symbol-function 'save-catalog)
                 (lambda () (error "injected catalog save failure"))
                 (symbol-function 'ensure-current-table)
                 (lambda (current)
                   (declare (ignore current))
                   t))
           (handler-case (publish-analyze-statistics table candidate)
             (error (condition) (setf caught condition)))
           (check (typep caught 'error)
                  "ANALYZE reports a catalog save failure")
           (check (eq (pg-class-statistics table) previous)
                  "ANALYZE restores in-memory statistics when persistence fails"))
      (setf (symbol-function 'save-catalog) original-save
            (symbol-function 'ensure-current-table) original-ensure))))

(defun test-analyze-publish-keeps-durable-stats-on-late-failure ()
  (let* ((table (make-pg-class :oid 62 :name "stats_durable_failure"
                               :relfilenode 6201 :columns #()))
         (previous (make-pg-table-statistics :row-count 1))
         (candidate (make-pg-table-statistics :row-count 2))
         (original-save (symbol-function 'save-catalog))
         (original-ensure (symbol-function 'ensure-current-table))
         (*wal-catalog-flush-generation* (wal-catalog-flush-generation))
         (caught nil))
    (setf (pg-class-statistics table) previous)
    (unwind-protect
         (progn
           (setf (symbol-function 'save-catalog)
                 (lambda ()
                   (incf *wal-catalog-flush-generation*)
                   (error "injected post-WAL catalog failure"))
                 (symbol-function 'ensure-current-table)
                 (lambda (current)
                   (declare (ignore current))
                   t))
           (handler-case (publish-analyze-statistics table candidate)
             (error (condition) (setf caught condition)))
           (check (typep caught 'error)
                  "ANALYZE reports a catalog failure after durable WAL")
           (check (eq (pg-class-statistics table) candidate)
                  "ANALYZE keeps statistics that are already present in durable WAL"))
      (setf (symbol-function 'save-catalog) original-save
            (symbol-function 'ensure-current-table) original-ensure))))

(defun test-nested-loop-seq-cost-models-replayable-inner ()
  (let* ((table (make-pg-class
                 :oid 61 :name "spooled_inner" :relfilenode 6101 :columns #()
                 :statistics (make-pg-table-statistics :row-count 100)))
         (path (make-select-scan-path :kind :seq-scan))
         (base-cost (select-scan-path-cost table path))
         (repeated-cost (nested-loop-inner-path-cost table path 5))
         (index (make-pg-index :oid 6102 :relfilenode 6102
                               :table-oid 61 :columns '(1)))
         (cached-index-path
           (make-select-scan-path :kind :index-scan :index index
                                  :selectivity 0.1d0))
         (parameterized-index-path
           (make-select-scan-path :kind :index-scan :index index
                                  :parameter-expr '(:col 1) :selectivity 0.1d0)))
    (check (= repeated-cost (+ base-cost 4.0d0))
           "SeqScan inner costing scans once and charges replay per later outer row")
    (check (< repeated-cost (* 5 base-cost))
           "replayable SeqScan costing is below a full rescan per outer row")
    (check (< (nested-loop-inner-path-cost table cached-index-path 5)
              (nested-loop-inner-path-cost table parameterized-index-path 5))
           "unparameterized IndexScan is spooled while parameterized scans rescan")
    (let ((*query-work-memory-bytes* 1))
      (check (= (select-path-spool-spill-page-cost table path) 1.0d0)
             "nested-loop costing estimates the disk pages for a spill")
      (check (= (- (nested-loop-inner-path-cost table path 5) base-cost) 9.0d0)
             "nested-loop costing charges the initial spill write and later rereads"))))
