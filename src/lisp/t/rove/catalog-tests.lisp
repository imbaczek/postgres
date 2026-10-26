;;;; catalog-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-key-order
    (flet ((k (v ty) (encode-index-key (list v) (list ty))))
      (rove:ok (minusp (key-compare (k -1 :int4) (k 0 :int4))) "int4 order")
      (rove:ok (minusp (key-compare (k -2147483648 :int4) (k 2147483647 :int4)))
               "int4 extremes")
      (rove:ok (minusp (key-compare (k -1 :int8) (k 0 :int8))) "int8 order")
      (rove:ok (minusp (key-compare (k +null+ :int4) (k -2147483648 :int4)))
               "null sorts first")
      (rove:ok (minusp (key-compare (k "abc" :varchar) (k "abd" :varchar)))
               "varchar order")
      ;; Index keys carry a length prefix, so ordering is length-first.  This
      ;; is internal only: SQL comparisons and ORDER BY use compare-3way.
      (rove:ok (minusp (key-compare (k "b" :varchar) (k "aa" :varchar)))
               "varchar key encoding is length-first")))

(rove:deftest test-index-catalog-roundtrip
    (let* ((catalog (make-catalog))
           (identity (catalog-cluster-identity catalog))
           (copy (catalog-from-sexp (catalog-to-sexp catalog))))
      (rove:ok (and (stringp identity)
                    (string= identity (catalog-cluster-identity copy)))
               "catalog incarnation survives durable catalog serialization"))
  (let* ((idx (make-pg-index :oid 42 :name "nnd" :relfilenode 42
                             :table-oid 41 :columns '(1) :unique-p t
                             :constraint-p t
                             :nulls-not-distinct-p t :valid-p t))
         (sexp (index-to-sexp idx))
         (copy (index-from-sexp sexp)))
    (rove:ok (equal sexp
                    '(:oid 42 :name "nnd" :relfilenode 42 :table-oid 41
                      :columns (1) :unique t :primary nil :constraint t
                      :nulls-not-distinct t :valid t))
             "index catalog encoding is the compatible flat plist")
    (rove:ok (pg-index-nulls-not-distinct-p copy)
             "NULLS NOT DISTINCT survives catalog serialization")
    (rove:ok (pg-index-constraint-p copy)
             "constraint ownership survives catalog serialization")
    (rove:ok (pg-index-valid-p copy)
             "index validity survives catalog serialization"))
  (let* ((class (make-pg-class
                 :oid 50 :name "codec" :relfilenode 51
                 :columns (vector (make-pg-column :name "value" :type :int4
                                                  :typmod -1 :attnum 1
                                                  :notnull t))
                 :indexes '(52)))
         (sexp (class-to-sexp class))
         (copy (class-from-sexp sexp)))
    (rove:ok (equal sexp
                    `(:oid 50 :name "codec" :relfilenode 51
                      :relfrozenxid ,+first-normal-xid+
                      :columns ((:name "value" :type :int4 :typmod -1
                                 :attnum 1 :notnull t :default-sequence nil))
                      :indexes (52)
                      :statistics nil))
             "class and column codecs use compatible flat plists")
    (rove:ok (vectorp (pg-class-columns copy))
             "class codec restores columns as a vector")
    (rove:ok (string= (pg-column-name (aref (pg-class-columns copy) 0)) "value")
             "class codec round-trips column mappings"))
  (let ((legacy-pk (index-from-sexp '(:primary t))))
    (rove:ok (not (pg-index-constraint-p legacy-pk))
             "legacy catalog has no constraint ownership flag")
    (rove:ok (not (pg-index-valid-p legacy-pk))
             "legacy indexes without a validity flag are not trusted")
    (rove:ok (pg-index-constraint-owned-p legacy-pk)
             "legacy primary index remains constraint-owned")))

(rove:deftest test-table-statistics-catalog-roundtrip
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
      (rove:ok (= (pg-table-statistics-row-count restored) 6)
               "ANALYZE row count persists through the class catalog codec")
      (rove:ok (= (pg-table-statistics-sample-count restored) 3)
               "ANALYZE sample count persists without storing raw sample rows")
      (rove:ok (= (pg-column-statistics-distinct-count
                   (aref (pg-table-statistics-columns restored) 0))
                  3)
               "column distinct count persists through the class catalog codec")))

(rove:deftest test-analyze-byte-vector-statistics-and-planner
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
      (rove:ok (= (pg-column-statistics-distinct-count column) 3)
               "ANALYZE counts separately allocated equal byte vectors once")
      (rove:ok (= (pg-column-statistics-null-fraction column) (/ 1.0d0 8))
               "ANALYZE keeps NULL separate from byte-vector values")
      (rove:ok (= (cdr (assoc first-copy mcv
                              :test #'analyze-statistics-value-equal-p))
                  (/ 3.0d0 8))
               "ANALYZE records the byte-vector MCV frequency over all sample rows")
      (rove:ok (= (cdr (assoc other-copy mcv
                              :test #'analyze-statistics-value-equal-p))
                  (/ 2.0d0 8))
               "ANALYZE distinguishes byte vectors with different lengths")
      (rove:ok (= (cdr (assoc empty-copy mcv
                              :test #'analyze-statistics-value-equal-p))
                  (/ 2.0d0 8))
               "ANALYZE matches separately allocated empty byte vectors")
      (rove:ok (typep (car (first mcv)) '(vector (unsigned-byte 8)))
               "catalog image restores byte-vector element type")
      (rove:ok (= (planner-equality-selectivity
                   restored 1 (copy-seq first))
                  (/ 3.0d0 8))
               "planner matches a fresh byte vector against the persisted MCV")
      (rove:ok (= (planner-equality-selectivity
                   restored 1 (copy-seq other))
                  (/ 2.0d0 8))
               "planner preserves byte-vector MCV frequency after catalog reload")))

(rove:deftest test-analyze-byte-vector-hash-collisions-and-text-equality
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
      (rove:ok (= (pg-column-statistics-distinct-count collided) 3)
               "byte-vector collisions are resolved with content equality")
      (rove:ok (= (cdr (assoc first
                              (pg-column-statistics-most-common-values collided)
                              :test #'analyze-statistics-value-equal-p))
                  (/ 2.0d0 5))
               "colliding byte-vector buckets retain the correct repeated count")
      (rove:ok (= (pg-column-statistics-distinct-count text-column) 2)
               "ANALYZE preserves case-sensitive EQUAL semantics for strings")))

(rove:deftest test-analyze-value-estimate-counts-backing-storage
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
      (rove:ok (>= (analyze-value-memory-estimate adjustable-bytes) (+ 64 1024))
               "ANALYZE charges adjustable byte-vector capacity beyond its fill pointer")
      (rove:ok (>= (analyze-value-memory-estimate adjustable-string) (+ 64 4096))
               "ANALYZE charges adjustable string capacity beyond its fill pointer")
      (rove:ok (>= (analyze-value-memory-estimate displaced-bytes) (+ 64 64 4096))
               "ANALYZE charges a displaced byte vector's full backing array")
      (rove:ok (>= (analyze-value-memory-estimate displaced-string) (+ 64 64 4000))
               "ANALYZE charges a displaced string's full backing array")))

(rove:deftest test-analyze-reservoir-shrink-clears-evicted-values
    (let* ((*analyze-random-function*
             (lambda (limit) (declare (ignore limit)) 0))
           (sample (make-array 4 :adjustable t :fill-pointer 4
                                 :initial-contents (vector :a :b :c :d))))
      (analyze-shrink-reservoir sample 2)
      (rove:ok (= (fill-pointer sample) 2)
               "ANALYZE shrink updates the reservoir fill pointer")
      (rove:ok (and (null (aref sample 2)) (null (aref sample 3)))
               "ANALYZE shrink clears references beyond the fill pointer")))
