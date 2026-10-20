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
                    :indexes (52)))
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
