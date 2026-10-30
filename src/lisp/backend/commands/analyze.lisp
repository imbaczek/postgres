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

(declaim (notinline sql-array-p sql-array-elements sql-array-dimensions sql-array-lower-bounds))

(declaim (ftype function compare-3way sql-group-value-payload))

(defun analyze-value-memory-estimate (value)
  (cond
    ((sql-array-p value)
     (+ 64 (analyze-value-memory-estimate (sql-array-elements value))
        (analyze-value-memory-estimate (sql-array-dimensions value))
        (analyze-value-memory-estimate (sql-array-lower-bounds value))))
    ((sql-jsonb-p value) (+ 32 (analyze-value-memory-estimate (sql-jsonb-value value))))
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
    ((sql-decimal-p value)
     (+ 32 (analyze-value-memory-estimate (sql-decimal-coefficient value))))
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

(defconstant +analyze-wide-value+ :analyze-wide-value
  "Marker for a non-NULL datum exceeding PostgreSQL's ANALYZE width cutoff.")

(defun analyze-sample-visible-rows (table relfilenode snapshot)
  "Count visible rows and retain an owned bounded physical tuple reservoir."
  (declare (ignore table))
  (let ((row-count 0)
        (sample (make-array +analyze-sample-size+ :adjustable t :fill-pointer 0)))
    (dotimes (block (buf-nblocks relfilenode))
      (check-for-interrupts)
      (let ((page (buf-read relfilenode block)))
        (when page
          (do-live-page-items (page offnum off len)
            (let ((xmin (full-xid-from-32 (page-u32 page (+ off +heap-tuple-xmin-off+))))
                  (xmax (full-xid-from-32 (page-u32 page (+ off +heap-tuple-xmax-off+))))
                  (cid (page-u32 page (+ off +heap-tuple-cid-off+)))
                  (infomask (page-u16 page (+ off +heap-tuple-infomask-off+))))
              (when (heap-tuple-visible-p xmin xmax cid snapshot
                                          (and *backend* (backend-xid *backend*)) infomask)
                (incf row-count)
                (analyze-add-reservoir-row (subseq page off (+ off len))
                                           row-count sample +analyze-sample-size+)))))))
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
  (cond ((and (sql-array-p left) (sql-array-p right))
         (zerop (compare-3way left right)))
        ((and (sql-jsonb-p left) (sql-jsonb-p right))
         (zerop (sql-jsonb-compare left right)))
        ((and (sql-timetz-p left) (sql-timetz-p right)) (zerop (sql-timetz-compare left right)))
        ((and (sql-interval-p left) (sql-interval-p right))
         (= (sql-interval-comparison-value left) (sql-interval-comparison-value right)))
        ((and (floatp left) (floatp right)) (zerop (sql-float-compare left right)))
        ((and (sql-numeric-p left) (sql-numeric-p right))
         (zerop (sql-numeric-compare left right)))
        ((and (analyze-byte-vector-p left) (analyze-byte-vector-p right))
         (equalp left right))
        (t (equal left right))))

(defun analyze-statistics-value-hash (value)
  (cond ((sql-array-p value) (sxhash (sql-group-value-payload value)))
        ((sql-jsonb-p value) (sxhash (sql-jsonb-equality-key value)))
        ((sql-timetz-p value) (sxhash (multiple-value-list (sql-timetz-comparison-values value))))
        ((sql-interval-p value) (sxhash (sql-interval-comparison-value value)))
        ((floatp value) (sxhash (sql-float-group-key value)))
        ((sql-numeric-special-p value) (sxhash value))
        ((sql-numeric-p value) (sxhash (sql-numeric-rational value)))
        ((analyze-byte-vector-p value) (analyze-byte-vector-content-hash value))
        (t (sxhash value))))

(defun analyze-finish-column-statistics (row-count sample-count null-count wide-count counts mcv)
  "Finish shared NULL and distinct estimates for logical or physical samples."
  (let* ((singletons (+ wide-count (count 1 counts :key #'cdr)))
         (doubletons (count 2 counts :key #'cdr))
         (nonnull (if (zerop sample-count) 0
                      (ceiling (* row-count (- sample-count null-count)) sample-count))))
    (make-pg-column-statistics
     :null-fraction (if (zerop sample-count) 0.0d0
                        (/ (float null-count 1.0d0) sample-count))
     :distinct-count (if (and (plusp wide-count) (null counts))
                         nonnull
                         (min nonnull (analyze-estimate-distinct-count
                                       row-count sample-count (+ wide-count (length counts))
                                       singletons doubletons)))
     :most-common-values mcv)))

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
        (null-count 0)
        (wide-count 0))
    (loop for row across sample
          for value = (aref row column-index)
          do (if (eq value +null+)
                 (incf null-count)
                 (if (eq value +analyze-wide-value+) (incf wide-count)
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
                             (push new-entry (gethash key frequencies))))))))
    (let* ((sample-count (length sample))
           (counts (nreverse values-in-order))
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
      (analyze-finish-column-statistics row-count sample-count null-count wide-count counts mcv))))

(defun analyze-physical-column-value (table bytes index snapshot)
  "Decode only INDEX from an owned sampled tuple under the parent snapshot."
  (let* ((values (deform-heap-tuple bytes (pg-class-columns table) :demand index
                                                                   :max-varlena-size 1024 :wide-value +analyze-wide-value+))
         (tuple (make-htup :values values)))
    (materialize-select-tuple table tuple snapshot :demand (cons table (list (1+ index))))
    (aref values index)))

(defun analyze-physical-column-statistics (table sample index row-count snapshot remaining
                                           &key (value-hash #'analyze-statistics-value-hash))
  "Count values using physical tuple references for representatives.
REMAINING limits decoded MCV values retained across all columns."
  (let ((buckets (make-hash-table :test #'equal))
        (entries nil) (null-count 0) (wide-count 0))
    (loop for bytes across sample for row-index from 0
          for value = (analyze-physical-column-value table bytes index snapshot)
          do (check-for-interrupts)
             (cond ((eq value +null+) (incf null-count))
                   ((eq value +analyze-wide-value+) (incf wide-count))
                   (t (let* ((key (list (if (analyze-byte-vector-p value) :byte-vector :equal)
                                        (funcall value-hash value)))
                             (entry (find-if
                                     (lambda (candidate)
                                       (analyze-statistics-value-equal-p
                                        value (analyze-physical-column-value
                                               table (aref sample (car candidate)) index snapshot)))
                                     (gethash key buckets))))
                        (if entry (incf (cdr entry))
                            (let ((new (cons row-index 1)))
                              (push new entries)
                              (push new (gethash key buckets))))))))
    (let* ((count (length sample))
           (ordered (nreverse entries))
           (ranked (stable-sort (remove-if-not (lambda (entry) (> (cdr entry) 1)) (copy-list ordered))
                                #'> :key #'cdr))
           (mcv nil))
      (loop for (row-index . frequency) in ranked for rank from 0 below +analyze-mcv-count+
            for value = (analyze-physical-column-value table (aref sample row-index) index snapshot)
            for size = (analyze-value-memory-estimate value)
            when (<= size (car remaining))
              do (decf (car remaining) size)
                 (push (cons value (/ (float frequency 1.0d0) (max 1 count))) mcv))
      (analyze-finish-column-statistics row-count count null-count wide-count ordered (nreverse mcv)))))

(defun analyze-table-statistics (table relfilenode snapshot)
  "Sample owned physical tuples and derive each column independently."
  (multiple-value-bind (row-count sample)
      (analyze-sample-visible-rows table relfilenode snapshot)
    (let* ((columns (pg-class-columns table))
           (statistics (make-array (length columns)))
           (remaining (list +analyze-sample-memory-bytes+)))
      (dotimes (index (length columns))
        (setf (aref statistics index)
              (if (member (pg-column-type (aref columns index)) '(:json :json-array))
                  (let ((null-count
                          (count-if (lambda (bytes)
                                      (and (logtest (page-u16 bytes +heap-tuple-infomask-off+) +heap-hasnull+)
                                           (logbitp (mod index 8)
                                                    (aref bytes (+ +size-of-heap-tuple-header+ (floor index 8))))))
                                    sample)))
                    (make-pg-column-statistics
                     :null-fraction (if (zerop (length sample)) 0.0d0
                                        (/ (float null-count 1.0d0) (length sample)))
                     :distinct-count 0 :most-common-values nil))
                  (analyze-physical-column-statistics table sample index row-count snapshot remaining))))
      (make-pg-table-statistics :row-count row-count :sample-count (length sample) :columns statistics))))
