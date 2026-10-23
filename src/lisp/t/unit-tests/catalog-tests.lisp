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
  (let* ((catalog (make-catalog))
         (identity (catalog-cluster-identity catalog))
         (copy (catalog-from-sexp (catalog-to-sexp catalog))))
    (check (and (stringp identity)
                (string= identity (catalog-cluster-identity copy)))
           "catalog incarnation survives durable catalog serialization"))
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
                               :attnum 1 :notnull t :default-sequence nil))
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

(defun test-analyze-catalog-class-image-roundtrip (table)
  "Round-trip TABLE through the same readable catalog image used on disk."
  (let* ((*catalog* (make-catalog))
         (database (make-database :name "postgres" :oid 1)))
    (setf (gethash "postgres" (catalog-databases *catalog*)) database
          (gethash (pg-class-name table) (database-tables database)) table)
    (let* ((image (bytes->string (catalog-image-bytes)))
           (restored
             (with-standard-io-syntax
               (let ((*read-eval* nil))
                 (catalog-from-sexp (read-from-string image)))))
           (restored-database
             (gethash "postgres" (catalog-databases restored))))
      (gethash (pg-class-name table) (database-tables restored-database)))))

(defun test-analyze-byte-vector-statistics-and-planner ()
  (let* ((first (make-array 3 :element-type '(unsigned-byte 8)
                              :initial-contents '(0 255 1)))
         (first-copy (copy-seq first))
         (other (make-array 4 :element-type '(unsigned-byte 8)
                              :initial-contents '(0 255 1 2)))
         (other-copy (copy-seq other))
         (empty (make-array 0 :element-type '(unsigned-byte 8)))
         (empty-copy (copy-seq empty))
         (sample (vector (vector first) (vector first-copy) (vector (copy-seq first))
                         (vector other) (vector other-copy) (vector empty)
                         (vector empty-copy) (vector +null+)))
         (column (analyze-column-statistics sample 0 8))
         (table (make-pg-class
                 :oid 5703 :name "byte_statistics" :relfilenode 5703
                 :columns #()
                 :statistics
                 (make-pg-table-statistics
                  :row-count 8 :sample-count 8 :columns (vector column))))
         (restored (test-analyze-catalog-class-image-roundtrip table))
         (restored-column
           (aref (pg-table-statistics-columns
                  (pg-class-statistics restored))
                 0))
         (mcv (pg-column-statistics-most-common-values restored-column)))
    (check (= (pg-column-statistics-distinct-count column) 3)
           "ANALYZE counts separately allocated equal byte vectors once")
    (check (= (pg-column-statistics-null-fraction column) (/ 1.0d0 8))
           "ANALYZE keeps NULL separate from byte-vector values")
    (check (= (cdr (assoc first-copy mcv
                          :test #'analyze-statistics-value-equal-p))
              (/ 3.0d0 8))
           "ANALYZE records the byte-vector MCV frequency over all sample rows")
    (check (= (cdr (assoc other-copy mcv
                          :test #'analyze-statistics-value-equal-p))
              (/ 2.0d0 8))
           "ANALYZE distinguishes byte vectors with different lengths")
    (check (= (cdr (assoc empty-copy mcv
                          :test #'analyze-statistics-value-equal-p))
              (/ 2.0d0 8))
           "ANALYZE matches separately allocated empty byte vectors")
    (check (typep (car (first mcv)) '(vector (unsigned-byte 8)))
           "catalog image restores byte-vector element type")
    (check (= (planner-equality-selectivity
               restored 1 (copy-seq first))
              (/ 3.0d0 8))
           "planner matches a fresh byte vector against the persisted MCV")
    (check (= (planner-equality-selectivity
               restored 1 (copy-seq other))
              (/ 2.0d0 8))
           "planner preserves byte-vector MCV frequency after catalog reload")))

(defun test-analyze-byte-vector-hash-collisions-and-text-equality ()
  (let* ((first (make-array 2 :element-type '(unsigned-byte 8)
                              :initial-contents '(1 2)))
         (sample (vector (vector first) (vector (copy-seq first))
                         (vector (make-array 2 :element-type '(unsigned-byte 8)
                                               :initial-contents '(2 1)))
                         (vector (make-array 0 :element-type '(unsigned-byte 8)))
                         (vector +null+)))
         (collided
           (analyze-column-statistics
            sample 0 5 :value-hash (lambda (value) (declare (ignore value)) 0)))
         (text-column
           (analyze-column-statistics (vector #("A") #("a") #("A")) 0 3)))
    (check (= (pg-column-statistics-distinct-count collided) 3)
           "byte-vector collisions are resolved with content equality")
    (check (= (cdr (assoc first
                          (pg-column-statistics-most-common-values collided)
                          :test #'analyze-statistics-value-equal-p))
              (/ 2.0d0 5))
           "colliding byte-vector buckets retain the correct repeated count")
    (check (= (pg-column-statistics-distinct-count text-column) 2)
           "ANALYZE preserves case-sensitive EQUAL semantics for strings")))

(defun test-analyze-retained-byte-vector-payload (sample)
  "Count payload bytes in the simple byte vectors reachable from SAMPLE."
  (loop for row across sample
        for value = (aref row 0)
        when (analyze-byte-vector-p value)
          sum (array-total-size value)))

(defun test-analyze-value-estimate-counts-backing-storage ()
  (let* ((adjustable-bytes
           (make-array 1024 :element-type '(unsigned-byte 8)
                            :initial-element 0 :adjustable t :fill-pointer 4))
         (byte-backing
           (make-array 4096 :element-type '(unsigned-byte 8)
                            :initial-element 0))
         (displaced-bytes
           (make-array 8 :element-type '(unsigned-byte 8)
                         :displaced-to byte-backing :displaced-index-offset 512))
         (adjustable-string
           (make-array 1024 :element-type 'character :initial-element #\x
                            :adjustable t :fill-pointer 4))
         (string-backing (make-string 1000 :initial-element #\x))
         (displaced-string
           (make-array 10 :element-type 'character
                          :displaced-to string-backing :displaced-index-offset 20)))
    (check (>= (analyze-value-memory-estimate adjustable-bytes) (+ 64 1024))
           "ANALYZE charges adjustable byte-vector capacity beyond its fill pointer")
    (check (>= (analyze-value-memory-estimate adjustable-string) (+ 64 4096))
           "ANALYZE charges adjustable string capacity beyond its fill pointer")
    (check (>= (analyze-value-memory-estimate displaced-bytes) (+ 64 64 4096))
           "ANALYZE charges a displaced byte vector's full backing array")
    (check (>= (analyze-value-memory-estimate displaced-string) (+ 64 64 4000))
           "ANALYZE charges a displaced string's full backing array")))

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

(defun test-analyze-reservoir-shrink-clears-evicted-values ()
  (let* ((*analyze-random-function*
           (lambda (limit) (declare (ignore limit)) 0))
         (sample (make-array 4 :adjustable t :fill-pointer 4
                               :initial-contents (vector :a :b :c :d))))
    (analyze-shrink-reservoir sample 2)
    (check (= (fill-pointer sample) 2)
           "ANALYZE shrink updates the reservoir fill pointer")
    (check (and (null (aref sample 2)) (null (aref sample 3)))
           "ANALYZE shrink clears references beyond the fill pointer")))

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
