;;;; catalog-tests.lisp -- catalog and index ordering tests.

(in-package #:pg)

(defun test-key-order ()
  (flet ((k (v ty) (encode-index-key (list v) (list ty))))
    (check (minusp (key-compare (k -1 :int4) (k 0 :int4))) "int4 order")
    (check (minusp (key-compare (k -2147483648 :int4) (k 2147483647 :int4)))
           "int4 extremes")
    (check (minusp (key-compare (k -1 :int8) (k 0 :int8))) "int8 order")
    (check (minusp (key-compare (k +null+ :int4) (k -2147483648 :int4)))
           "null sorts first")
    (check (minusp (key-compare (k "abc" :varchar) (k "abd" :varchar)))
           "varchar order")
    ;; Index keys carry a length prefix, so ordering is length-first.  This
    ;; is internal only: SQL comparisons and ORDER BY use compare-3way.
    (check (minusp (key-compare (k "b" :varchar) (k "aa" :varchar)))
           "varchar key encoding is length-first")))

(defun test-index-catalog-roundtrip ()
  (let* ((idx (make-pg-index :oid 42 :name "nnd" :relfilenode 42
                             :table-oid 41 :columns '(1) :unique-p t
                             :constraint-p t
                             :nulls-not-distinct-p t :valid-p t))
         (sexp (index-to-sexp idx))
         (copy (index-from-sexp sexp)))
    (check (equal sexp
                  '(:oid 42 :name "nnd" :relfilenode 42 :table-oid 41
                    :columns (1) :unique t :primary nil :constraint t
                    :nulls-not-distinct t :valid t))
           "index catalog encoding is the compatible flat plist")
    (check (pg-index-nulls-not-distinct-p copy)
           "NULLS NOT DISTINCT survives catalog serialization")
    (check (pg-index-constraint-p copy)
           "constraint ownership survives catalog serialization")
    (check (pg-index-valid-p copy)
           "index validity survives catalog serialization"))
  (let* ((class (make-pg-class
                 :oid 50 :name "codec" :relfilenode 51
                 :columns (vector (make-pg-column :name "value" :type :int4
                                                  :typmod -1 :attnum 1
                                                  :notnull t))
                 :indexes '(52)))
         (sexp (class-to-sexp class))
         (copy (class-from-sexp sexp)))
    (check (equal sexp
                  `(:oid 50 :name "codec" :relfilenode 51
                    :relfrozenxid ,+first-normal-xid+
                    :columns ((:name "value" :type :int4 :typmod -1
                               :attnum 1 :notnull t))
                    :indexes (52)
                    :statistics nil))
           "class and column codecs use compatible flat plists")
    (check (vectorp (pg-class-columns copy))
           "class codec restores columns as a vector")
    (check (string= (pg-column-name (aref (pg-class-columns copy) 0)) "value")
           "class codec round-trips column mappings"))
  (let ((legacy-pk (index-from-sexp '(:primary t))))
    (check (not (pg-index-constraint-p legacy-pk))
           "legacy catalog has no constraint ownership flag")
    (check (not (pg-index-valid-p legacy-pk))
           "legacy indexes without a validity flag are not trusted")
    (check (pg-index-constraint-owned-p legacy-pk)
           "legacy primary index remains constraint-owned")))

(defun test-table-statistics-catalog-roundtrip ()
  (let* ((statistics
           (make-pg-table-statistics
            :row-count 6 :sample-count 3
            :columns
            (vector (make-pg-column-statistics
                     :null-fraction 0.25d0 :distinct-count 3
                     :most-common-values '((1 . 0.5d0))))))
         (table (make-pg-class :oid 55 :name "stats" :relfilenode 56
                               :columns #() :statistics statistics))
         (copy (class-from-sexp (class-to-sexp table)))
         (restored (pg-class-statistics copy)))
    (check (= (pg-table-statistics-row-count restored) 6)
           "ANALYZE row count persists through the class catalog codec")
    (check (= (pg-table-statistics-sample-count restored) 3)
           "ANALYZE sample count persists without storing raw sample rows")
    (check (= (pg-column-statistics-distinct-count
               (aref (pg-table-statistics-columns restored) 0))
              3)
           "column distinct count persists through the class catalog codec")))

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
