;;;; analyze.lisp -- bounded table samples and planner statistics.

(in-package #:pg)

(defconstant +analyze-sample-size+ 300)
(defconstant +analyze-sample-memory-bytes+ (* 4 1024 1024))
(defconstant +analyze-mcv-count+ 10)
(defvar *analyze-random-function* #'random)

(defun analyze-row-memory-estimate (row)
  "Conservatively estimate the retained Lisp objects for one sampled row."
  (+ 24
     (* 8 (length row))
     (loop for value across row
           sum (typecase value
                 (string (+ 16 (* 4 (length value))))
                 (integer 16)
                 (t 0)))))

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
number of samples that fit; the raw rows are not persisted."
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

(defun analyze-column-statistics (sample column-index row-count)
  "Derive NULL, distinct, and top-10 MCV estimates from one column sample."
  (let ((frequencies (make-hash-table :test #'equal))
        (values-in-order '())
        (null-count 0))
    (loop for row across sample
          for value = (aref row column-index)
          do (if (eq value +null+)
                 (incf null-count)
                 (multiple-value-bind (count present-p)
                     (gethash value frequencies)
                   (unless present-p
                     (push value values-in-order))
                   (setf (gethash value frequencies) (1+ (or count 0))))))
    (let* ((sample-count (length sample))
           (distinct-values (nreverse values-in-order))
           (counts (mapcar (lambda (value)
                             (cons value (gethash value frequencies)))
                           distinct-values))
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
