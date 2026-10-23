;;;; analyze.lisp -- bounded table samples and planner statistics.

(in-package #:pg)

(defconstant +analyze-sample-size+ 300)
(defconstant +analyze-sample-memory-bytes+ (* 4 1024 1024))
(defconstant +analyze-mcv-count+ 10)
(defvar *analyze-random-function* #'random)

(defun analyze-array-memory-estimate (array element-bytes value-estimator
                                      &optional scan-elements-p)
  (multiple-value-bind (backing offset) (array-displacement array)
    (declare (ignore offset))
    (+ 64
       (if backing
           (funcall value-estimator backing)
           (* element-bytes (array-total-size array)))
       (if scan-elements-p
           (loop for index below (array-total-size array)
                 sum (funcall value-estimator
                              (row-major-aref array index)))
           0))))

(defun analyze-value-memory-estimate (value)
  (cond
    ((stringp value)
     ;; Use total size, not LENGTH, so adjustable/fill-pointer strings are
     ;; charged for the storage retained by the sampled row.
     (analyze-array-memory-estimate value 4 #'analyze-value-memory-estimate))
    ((and (vectorp value)
          (typep value '(vector (unsigned-byte 8))))
     ;; BYTEA equality and storage both operate on octets.  Array displacement
     ;; and adjustable capacity matter because sampled rows are shallow copies.
     (analyze-array-memory-estimate value 1 #'analyze-value-memory-estimate))
    ((vectorp value)
     (analyze-array-memory-estimate value 8 #'analyze-value-memory-estimate t))
    ((consp value)
     (+ 48
        (analyze-value-memory-estimate (car value))
        (analyze-value-memory-estimate (cdr value))))
    ((integerp value)
     (if (typep value 'fixnum)
         0
         (+ 16 (ceiling (integer-length value) 8))))
    ((rationalp value)
     (+ 32
        (analyze-value-memory-estimate (numerator value))
        (analyze-value-memory-estimate (denominator value))))
    ((floatp value) 32)
    ((complexp value)
     (+ 32
        (analyze-value-memory-estimate (realpart value))
        (analyze-value-memory-estimate (imagpart value))))
    ((symbolp value)
     (if (symbol-package value)
         0
         (+ 32 (* 2 (length (symbol-name value))))))
    (t 64)))

(defun analyze-row-memory-estimate (row)
  "Estimate retained row bytes for the values the heap decoder can produce.

PostgreSQL's analyze.c:compute_distinct_stats accounts for variable-width
datums and avoids detoasting values wider than its statistics threshold.  This
Lisp path also limits the reservoir's retained shallow-copy payload to 4 MiB."
  (+ 24
     (* 8 (length row))
     (loop for value across row
           sum (analyze-value-memory-estimate value))))

(defun analyze-sample-capacity (maximum-row-bytes)
  (if (zerop maximum-row-bytes)
      +analyze-sample-size+
      (max 1 (min +analyze-sample-size+
                  (floor +analyze-sample-memory-bytes+ maximum-row-bytes)))))

(defun analyze-shrink-reservoir (sample capacity)
  "Uniformly reduce SAMPLE to CAPACITY, clearing discarded row references."
  (loop while (> (fill-pointer sample) capacity)
        for last = (1- (fill-pointer sample))
        for slot = (funcall *analyze-random-function* (fill-pointer sample))
        do (setf (aref sample slot) (aref sample last)
                 (aref sample last) nil)
           (decf (fill-pointer sample)))
  sample)

(defun analyze-add-reservoir-row (row seen sample capacity)
  "Add ROW to a fixed-size Algorithm R reservoir after SEEN eligible rows."
  (if (< (fill-pointer sample) capacity)
      (vector-push-extend (copy-seq row) sample)
      (let ((slot (funcall *analyze-random-function* seen)))
        (when (< slot capacity)
          (setf (aref sample slot) (copy-seq row)))))
  sample)

(defun analyze-sample-visible-rows (table relfilenode snapshot)
  "Count visible rows exactly and retain a bounded reservoir sample.

This follows PostgreSQL's analyze.c:acquire_sample_rows shape: a streaming heap
scan with reservoir replacement, so early pages do not dominate the sample.
Rows are copied only while both the 300-row and 4 MiB retained-object budgets
allow them. Reservoir capacity shrinks uniformly when wider rows reduce the
number of samples that fit; the raw rows are not persisted. Unlike PostgreSQL's
compute_distinct_stats, which skips only a too-wide datum, this path excludes
an entire row when its estimated copy exceeds 4 MiB. That can bias statistics
for every column. The limit estimates retained sample storage; it is not an
exact SBCL heap cap and does not include transient decode allocations."
  (let ((row-count 0)
        (eligible-row-count 0)
        (maximum-row-bytes 0)
        (sample-capacity +analyze-sample-size+)
        (sample (make-array +analyze-sample-size+
                            :adjustable t :fill-pointer 0)))
    (heapam-scan relfilenode (pg-class-columns table) snapshot
                 (lambda (tuple)
                   (incf row-count)
                   (let* ((values (htup-values tuple))
                          (row-bytes (analyze-row-memory-estimate values)))
                     (when (<= row-bytes +analyze-sample-memory-bytes+)
                       (setf maximum-row-bytes (max maximum-row-bytes row-bytes))
                       (let ((new-capacity
                               (analyze-sample-capacity maximum-row-bytes)))
                         (when (< new-capacity sample-capacity)
                           (analyze-shrink-reservoir sample new-capacity)
                           (setf sample-capacity new-capacity)))
                       (incf eligible-row-count)
                       (analyze-add-reservoir-row
                        values eligible-row-count sample sample-capacity)))))
    (values row-count (coerce sample 'vector))))

(defun analyze-estimate-distinct-count (row-count sample-count observed
                                        singleton-count doubleton-count)
  "Estimate distinct non-NULL values with a bounded Chao-style correction."
  (cond
    ((zerop sample-count) 0)
    ((<= row-count sample-count) observed)
    (t
     (let* ((unseen
              (if (plusp doubleton-count)
                  (ceiling (* singleton-count singleton-count)
                           (* 2 doubleton-count))
                  (floor (* singleton-count (max 0 (1- singleton-count))) 2)))
            (population (max 0 row-count)))
       (min population (+ observed unseen))))))

(defun analyze-byte-vector-p (value)
  (and (vectorp value)
       (typep value '(vector (unsigned-byte 8)))))

(defun analyze-byte-vector-content-hash (value)
  "Return a bounded content hash; bucket equality still checks every octet."
  (loop with hash = #x811C9DC5
        for byte across value
        do (setf hash
                 (logand #xFFFFFFFF
                         (* (logxor hash byte) #x01000193)))
        finally (return (logand #xFFFFFFFF
                                (logxor hash (length value))))))

(defun analyze-statistics-value-equal-p (left right)
  ;; PostgreSQL's analyze.c:compute_distinct_stats calls the column's equality
  ;; operator; bytea's byteaeq compares length and contents (bytea.c:814-844).
  ;; Keep EQUAL for other values so text remains case-sensitive.
  (if (and (analyze-byte-vector-p left)
           (analyze-byte-vector-p right))
      (equalp left right)
      (equal left right)))

(defun analyze-statistics-value-hash (value)
  (if (analyze-byte-vector-p value)
      (analyze-byte-vector-content-hash value)
      (sxhash value)))

(defun analyze-column-statistics (sample column-index row-count
                                  &key
                                    (value-hash
                                     #'analyze-statistics-value-hash))
  "Derive NULL, distinct, and top-10 MCV estimates from one column sample.

BYTEA-like vectors use content hashes and collision-checked buckets, matching
the type-specific equality semantics used by PostgreSQL's analyze.c.  Other
values retain the existing EQUAL behavior.  VALUE-HASH permits a forced
collision test without changing the production hash implementation."
  (let ((frequencies (make-hash-table :test #'equal))
        (values-in-order '())
        (null-count 0))
    (loop for row across sample
          for value = (aref row column-index)
          do (if (eq value +null+)
                 (incf null-count)
                 (let* ((key (list (if (analyze-byte-vector-p value)
                                       :byte-vector
                                       :equal)
                                   (funcall value-hash value)))
                        (bucket (gethash key frequencies))
                        (entry (find value bucket
                                     :key #'car
                                     :test #'analyze-statistics-value-equal-p)))
                   (if entry
                       (incf (cdr entry))
                       (let ((new-entry (cons value 1)))
                         (push new-entry values-in-order)
                         (push new-entry (gethash key frequencies)))))))
    (let* ((sample-count (length sample))
           (counts (nreverse values-in-order))
           (singletons (count 1 counts :key #'cdr))
           (doubletons (count 2 counts :key #'cdr))
           (mcv-counts
             (stable-sort
              (remove-if-not (lambda (entry) (> (cdr entry) 1))
                             (copy-list counts))
              #'> :key #'cdr))
           (mcv
             (loop for (value . count) in mcv-counts
                   for rank from 0 below +analyze-mcv-count+
                   collect (cons value (/ (float count 1.0d0)
                                          (max 1 sample-count))))))
      (make-pg-column-statistics
       :null-fraction (if (zerop sample-count)
                          0.0d0
                          (/ (float null-count 1.0d0) sample-count))
       :distinct-count
       (analyze-estimate-distinct-count
        row-count sample-count (length counts) singletons doubletons)
       :most-common-values mcv))))

(defun analyze-table-statistics (table relfilenode snapshot)
  "Persist derived statistics after analyzing at most 300 sampled rows."
  (multiple-value-bind (row-count sample)
      (analyze-sample-visible-rows table relfilenode snapshot)
    (make-pg-table-statistics
     :row-count row-count
     :sample-count (length sample)
     :columns (loop for index below (length (pg-class-columns table))
                    collect (analyze-column-statistics
                             sample index row-count)
                      into columns
                    finally (return (coerce columns 'vector))))))
