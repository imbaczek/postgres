;;;; exec_scan.lisp -- SELECT access-path selection and scan execution.

(in-package #:pg)

(defmacro do-table-tuples ((tuple table &key relfilenode) &body body)
  "Run BODY for visible tuples in TABLE, checking interrupts per tuple.

An already-bound RELFILENODE may be supplied when relation registration must
precede scanning. Registration and locking remain the caller's responsibility."
  (let ((table-value (gensym "TABLE-"))
        (rel (gensym "RELFILENODE-"))
        (columns (gensym "COLUMNS-"))
        (snapshot (gensym "SNAPSHOT-")))
    `(let* ((,table-value ,table)
            (,rel ,(or relfilenode `(pg-class-relfilenode ,table-value)))
            (,columns (pg-class-columns ,table-value))
            (,snapshot *current-snapshot*))
       (heapam-scan ,rel ,columns ,snapshot
                    (lambda (,tuple)
                      (check-for-interrupts)
                      ,@body)))))

(defmacro with-registered-relation-access
    ((relfilenodes xid mode) relation-expression validation-form &body body)
  "Register relation users before validating catalog state under *WRITE-LOCK*.

MODE is the literal :READ or :WRITE. Readers register the complete set
atomically; writers must register exactly one relation."
  (let* ((captured-relfilenodes (gensym "CAPTURED-RELFILENODES-"))
         (register-form
           (ecase mode
             (:read
              `(register-relation-readers ,captured-relfilenodes ,xid))
             (:write
              `(progn
                 (unless (= (length ,captured-relfilenodes) 1)
                   (error "write registration requires exactly one relation"))
                 (register-relation-writer
                  (first ,captured-relfilenodes) ,xid))))))
    `(let* ((,captured-relfilenodes
              (remove-duplicates (copy-list ,relation-expression) :test #'=))
            (,relfilenodes ,captured-relfilenodes)
            (,xid (get-current-xid)))
       (declare (ignorable ,relfilenodes ,xid))
       ,register-form
       (with-write-lock ,validation-form)
       ,@body)))

(defmacro with-registered-table ((table relfilenode xid mode) &body body)
  "Register TABLE for the transaction, then revalidate it under the write lock.

MODE is the literal :READ or :WRITE. Registration remains until the XID ends."
  (let ((table-value (gensym "TABLE-"))
        (relfilenode-value (gensym "RELFILENODE-"))
        (registered-relfilenodes (gensym "REGISTERED-RELFILENODES-")))
    `(let* ((,table-value ,table)
            (,relfilenode-value (pg-class-relfilenode ,table-value)))
       (with-registered-relation-access
           (,registered-relfilenodes ,xid ,mode)
           (list ,relfilenode-value)
           (ensure-current-table ,table-value)
         (let ((,relfilenode ,relfilenode-value))
           ,@body)))))

(defun target->result-column (target)
  (let ((oid (type-oid (getf target :type))))
    (make-result-column :name (getf target :name)
                        :table-oid (getf target :table-oid)
                        :attnum (getf target :attnum)
                        :type-oid oid
                        :typlen (type-typlen oid)
                        :typmod (getf target :typmod)
                        :format 0)))

(defun order-compare (a b)
  (cond
    ((and (eq a +null+) (eq b +null+)) 0)
    ((eq a +null+) 1)
    ((eq b +null+) -1)
    (t (compare-3way a b))))

(defun row-less-p (a b order)
  (loop for (expr . desc) in order
        do (let* ((va (eval-expr expr (cdr a)))
                  (vb (eval-expr expr (cdr b)))
                  (c (order-compare va vb)))
             (unless (zerop c)
               (return (if desc (> c 0) (< c 0)))))
        finally (return nil)))

(defstruct select-top-n-item
  row
  keys
  sequence)

(defstruct (select-top-n-collector (:constructor %make-select-top-n-collector))
  limit
  order
  heap
  next-sequence)

(defun make-select-top-n-collector (limit order)
  (%make-select-top-n-collector
   :limit limit
   :order order
   :heap (make-array (min limit 32) :adjustable t :fill-pointer 0)
   :next-sequence 0))

(defun select-top-n-push (heap item limit)
  (when (= (fill-pointer heap) (array-dimension heap 0))
    (setf heap
          (adjust-array heap
                        (min limit (max 1 (* 2 (array-dimension heap 0))))
                        :fill-pointer (fill-pointer heap))))
  (vector-push item heap)
  heap)

(defun select-top-n-item-less-p (a b order)
  (loop for entry in order
        for position from 0
        for c = (order-compare (aref (select-top-n-item-keys a) position)
                               (aref (select-top-n-item-keys b) position))
        unless (zerop c)
          return (if (cdr entry) (> c 0) (< c 0))
        finally
           (return (< (select-top-n-item-sequence a)
                      (select-top-n-item-sequence b)))))

(defun select-top-n-sift-up (heap order index)
  (when (plusp index)
    (let ((parent (floor (1- index) 2)))
      (when (select-top-n-item-less-p (aref heap parent) (aref heap index)
                                      order)
        (rotatef (aref heap index) (aref heap parent))
        (select-top-n-sift-up heap order parent)))))

(defun select-top-n-sift-down (heap order index)
  (let* ((left (+ (* 2 index) 1))
         (right (1+ left))
         (heap-length (length heap)))
    (when (< left heap-length)
      (let* ((worse-child
               (if (and (< right heap-length)
                        (select-top-n-item-less-p (aref heap left)
                                                  (aref heap right) order))
                   right
                   left)))
        (when (select-top-n-item-less-p (aref heap index)
                                        (aref heap worse-child) order)
          (rotatef (aref heap index) (aref heap worse-child))
          (select-top-n-sift-down heap order worse-child))))))

(defun offer-select-top-n-row (collector row)
  (let* ((order (select-top-n-collector-order collector))
         (heap (select-top-n-collector-heap collector))
         (limit (select-top-n-collector-limit collector))
         (sequence (select-top-n-collector-next-sequence collector))
         (item (make-select-top-n-item
                :row row
                :keys (map 'vector
                           (lambda (entry)
                             (eval-expr (car entry) (cdr row)))
                           order)
                :sequence sequence)))
    (incf (select-top-n-collector-next-sequence collector))
    (cond
      ((< (length heap) limit)
       (setf (select-top-n-collector-heap collector)
             (select-top-n-push heap item limit))
       (setf heap (select-top-n-collector-heap collector))
       (select-top-n-sift-up heap order (1- (length heap))))
      ((and (plusp limit)
            (select-top-n-item-less-p item (aref heap 0) order))
       (setf (aref heap 0) item)
       (select-top-n-sift-down heap order 0)))))

(defun select-top-n-collector-rows (collector)
  (mapcar #'select-top-n-item-row
          (stable-sort (coerce (select-top-n-collector-heap collector) 'list)
                       (lambda (a b)
                         (select-top-n-item-less-p
                          a b (select-top-n-collector-order collector))))))

(defun collect-select-top-n (streamer order limit)
  "Consume STREAMER while retaining at most LIMIT rows in ORDER BY order."
  (let ((collector (make-select-top-n-collector limit order)))
    (unless (zerop limit)
      (funcall streamer (lambda (row) (offer-select-top-n-row collector row))))
    (select-top-n-collector-rows collector)))

(defstruct select-scan-path
  (kind :seq-scan)
  index
  qual
  key
  parameter-expr
  selectivity)

(defun where-conjuncts (where)
  "Return WHERE's top-level AND terms in source order."
  (if (and where (eq (first where) :and))
      (append (where-conjuncts (second where))
              (where-conjuncts (third where)))
      (if where (list where) nil)))

(defun simple-equality-constant (expr)
  (case (first expr)
    (:const
     (values (second expr) t))
    (:unary
     (let ((value (third expr)))
       (when (and (string= (second expr) "-")
                  (eq (first value) :const)
                  (integerp (second value)))
         (values (- (second value)) t))))))

(defun equality-column-constant (qual)
  (when (and (eq (first qual) :binop)
             (string= (second qual) "="))
    (let ((left (third qual))
          (right (fourth qual)))
      (multiple-value-bind (value constant-p)
          (simple-equality-constant right)
        (when (and (eq (first left) :col) constant-p)
          (return-from equality-column-constant
            (values (second left) value t))))
      (multiple-value-bind (value constant-p)
          (simple-equality-constant left)
        (when (and (eq (first right) :col) constant-p)
          (values (second right) value t))))))

(defun indexable-equality-value-p (value type)
  (and (not (eq value +null+))
       (case type
         ((:int4 :int8) (integerp value))
         (:varchar (stringp value))
         (t nil))))

(defun table-statistics-column (table attnum)
  (let* ((statistics (pg-class-statistics table))
         (columns (and statistics (pg-table-statistics-columns statistics))))
    (when (and columns (<= 1 attnum (length columns)))
      (aref columns (1- attnum)))))

(defun table-row-estimate (table)
  (let ((statistics (pg-class-statistics table)))
    (if statistics
        (pg-table-statistics-row-count statistics)
        (* 100 (buf-nblocks (pg-class-relfilenode table))))))

(defun table-page-estimate (table)
  (max 1 (buf-nblocks (pg-class-relfilenode table))))

(defun planner-equality-selectivity (table attnum value)
  "Use ANALYZE's MCV frequencies, then its residual ndistinct estimate."
  (let ((statistics (table-statistics-column table attnum)))
    (if (null statistics)
        0.1d0
        (let* ((mcv (pg-column-statistics-most-common-values statistics))
               (common (and value
                            (assoc value mcv :test #'equal)))
               (common-fraction (reduce #'+ mcv :key #'cdr :initial-value 0.0d0))
               (remaining-values
                 (max 1 (- (pg-column-statistics-distinct-count statistics)
                           (length mcv))))
               (residual (max 0.0d0
                              (- 1.0d0
                                 (pg-column-statistics-null-fraction statistics)
                                 common-fraction))))
          (min 1.0d0
               (max 0.0d0
                    (or (cdr common)
                        (/ residual remaining-values))))))))

(defun index-paths-for-column (table attnum qual &key value parameter-expr)
  (when (<= 1 attnum (length (pg-class-columns table)))
    (loop for index in (table-indexes table)
          when (and (pg-index-valid-p index)
                    (= (length (pg-index-columns index)) 1)
                    (= (first (pg-index-columns index)) attnum))
            append
            (let* ((types (index-types index table))
                   (type (first types))
                   (constant-p (null parameter-expr)))
              (when (or (not constant-p)
                        (indexable-equality-value-p value type))
                (list
                 (make-select-scan-path
                  :kind :index-scan
                  :index index
                  :qual qual
                  :key (when constant-p
                         (encode-index-key (list value) types))
                  :parameter-expr parameter-expr
                  :selectivity
                  (let ((selectivity
                          (if constant-p
                              (planner-equality-selectivity table attnum value)
                              (let ((statistics
                                      (table-statistics-column table attnum)))
                                (if statistics
                                    (/ 1.0d0
                                       (max 1 (pg-column-statistics-distinct-count
                                               statistics)))
                                    0.1d0)))))
                    (if (pg-index-unique-p index)
                        (min selectivity
                             (/ 1.0d0 (max 1 (table-row-estimate table))))
                        selectivity)))))))))

(defun index-paths-for-equality (table qual &optional (column-offset 0))
  (multiple-value-bind (attnum value found-p)
      (equality-column-constant qual)
    (when (and found-p
               (< column-offset attnum)
               (<= attnum (+ column-offset
                             (length (pg-class-columns table)))))
      (index-paths-for-column table (- attnum column-offset) qual
                              :value value))))

(defun index-path-for-equality (table qual &optional (column-offset 0))
  (first (index-paths-for-equality table qual column-offset)))

(defun planner-random-heap-page-estimate (pages matching-rows)
  "Approximate random heap-page visits under uniform row placement.

costsize.c uses index_pages_fetched's cache-aware Mackert-Lohman estimate;
this Poisson approximation stays monotone and bounded for the small planner."
  (if (zerop matching-rows)
      0
      (min pages
           (ceiling (* pages
                       (- 1.0d0
                          (exp (- (/ matching-rows (float pages 1.0d0))))))))))

(defun select-scan-path-cost (table path)
  "Estimate SeqScan and IndexScan using costsize.c's page/tuple model.

The constants intentionally follow costsize.c's default sequential page,
random page, and tuple CPU costs; this is a stable first choice model rather
than a configurable cost GUC surface."
  (let* ((rows (table-row-estimate table))
         (pages (table-page-estimate table)))
    (if (eq (select-scan-path-kind path) :seq-scan)
        (+ pages (* rows 0.01d0))
        (let* ((index (select-scan-path-index path))
               (index-pages (max 1 (buf-nblocks (pg-index-relfilenode index))))
               (levels (max 1 (ceiling (log index-pages 100))))
               (selectivity (or (select-scan-path-selectivity path) 0.1d0))
               (index-rows (* rows selectivity))
               (heap-pages (planner-random-heap-page-estimate
                            pages index-rows)))
          (+ (* 4.0d0 levels)
             (* 4.0d0 heap-pages)
             (* 0.01d0 index-rows))))))

(defun select-scan-path-row-estimate (table path)
  (* (table-row-estimate table)
     (if (eq (select-scan-path-kind path) :index-scan)
         (or (select-scan-path-selectivity path) 0.1d0)
         1.0d0)))

(defun select-path-spool-spill-page-cost (table path)
  "Estimate sequential I/O pages for a materialized inner that spills.

Like costsize.c:cost_material, this charges a sequential page write at
materialization and a page read per replay. PostgreSQL BLCKSZ (8192 bytes)
approximates tuple spool size from relation pages and path selectivity."
  (let* ((selectivity (if (eq (select-scan-path-kind path) :index-scan)
                          (or (select-scan-path-selectivity path) 0.1d0)
                          1.0d0))
         (spool-bytes (* 8192 (table-page-estimate table) selectivity))
         (memory-limit (max 1 *query-work-memory-bytes*)))
    (if (> spool-bytes memory-limit)
        (/ spool-bytes 8192.0d0)
        0.0d0)))

(defun nested-loop-inner-path-cost (table path outer-rows)
  "Estimate an inner path using its parameterization and replay strategy.

Unparameterized SeqScan and IndexScan inputs are cached by
stream-select-join-environments, like nodeMaterial.c, so their access cost is
paid once and later passes pay tuple replay plus possible spill reads. A
parameterized IndexScan is rescanned per outer row, as in nodeIndexscan.c."
  (let* ((scan-cost (select-scan-path-cost table path))
         (loops (max 1.0d0 outer-rows))
         (spill-pages (select-path-spool-spill-page-cost table path)))
    (if (select-scan-path-parameter-expr path)
        (* loops scan-cost)
        (+ scan-cost
           ;; The first inner pass writes spilled tuples; each later pass reads
           ;; the same spool sequentially, matching nodeMaterial.c's rescan cost.
           spill-pages
           (* (max 0.0d0 (1- loops))
              (+ (* (select-scan-path-row-estimate table path) 0.01d0)
                 spill-pages))))))

(defun select-path-before-p (table left right &optional outer-rows)
  (if outer-rows
      (< (nested-loop-inner-path-cost table left outer-rows)
         (nested-loop-inner-path-cost table right outer-rows))
      (< (select-scan-path-cost table left)
         (select-scan-path-cost table right))))

(defun equality-column-references (qual)
  "Return the two column expressions in a simple equality qual."
  (when (and (eq (first qual) :binop)
             (string= (second qual) "=")
             (eq (first (third qual)) :col)
             (eq (first (fourth qual)) :col))
    (values (third qual) (fourth qual) t)))

(defun index-paths-for-join-equality (table qual outer-width right-offset)
  "Find parameterized indexes for right-side Vars equated to outer Vars.

Like indxpath.c's parameterized IndexPath discovery, only a simple equality
whose other operand belongs to the already-built outer relation is considered."
  (multiple-value-bind (left right found-p)
      (equality-column-references qual)
    (when found-p
      (loop for (right-var outer-var) in (list (list right left) (list left right))
            for global-attnum = (second right-var)
            for local-attnum = (- global-attnum right-offset)
            when (and (<= 1 local-attnum (length (pg-class-columns table)))
                      (<= 1 (second outer-var) outer-width))
              append (index-paths-for-column
                      table local-attnum qual
                      :parameter-expr outer-var)))))

(defun select-relation-index-for-attnum (attnum relations)
  (position-if
   (lambda (relation)
     (let ((offset (getf relation :offset))
           (width (getf relation :width)))
       (and (< offset attnum) (<= attnum (+ offset width)))))
   relations))

(defun index-paths-for-reordered-join-equality
    (table qual relation-index relations joined-indices)
  "Find a parameterized path from this relation to any already joined Var."
  (multiple-value-bind (left right found-p)
      (equality-column-references qual)
    (when found-p
      (loop for (right-var outer-var) in (list (list right left) (list left right))
            for local-attnum =
                             (- (second right-var)
                                (getf (nth relation-index relations) :offset))
            for outer-index =
                            (select-relation-index-for-attnum (second outer-var) relations)
            when (and (<= 1 local-attnum (length (pg-class-columns table)))
                      (member outer-index joined-indices))
              append (index-paths-for-column
                      table local-attnum qual
                      :parameter-expr outer-var)))))

(defun index-paths-for-relation-joins
    (table relation-index relations joined-indices joins)
  (loop for join in joins
        append (index-paths-for-reordered-join-equality
                table (getf join :qual) relation-index relations
                joined-indices)))

(defun choose-cheapest-select-path (table candidates &optional outer-rows)
  (reduce (lambda (best path)
            (if (select-path-before-p table path best outer-rows) path best))
          (rest candidates) :initial-value (first candidates)))

(defun choose-join-inner-path (table join outer-rows right-offset)
  (let* ((type (getf join :type))
         (paths (if (member type '(:inner :left))
                    (index-paths-for-join-equality
                     table (getf join :qual) (getf join :left-width)
                     right-offset)
                    nil))
         (seq (make-select-scan-path :kind :seq-scan)))
    (if (null paths)
        seq
        (let* ((best-index
                 (choose-cheapest-select-path table paths outer-rows))
               (index-cost (nested-loop-inner-path-cost
                            table best-index outer-rows))
               (seq-cost (nested-loop-inner-path-cost table seq outer-rows)))
          (if (< index-cost seq-cost) best-index seq)))))

(defun choose-select-scan-path (table where &optional (column-offset 0))
  "Choose the lowest-cost SeqScan or equality IndexScan.

COLUMN-OFFSET maps the query-wide Var numbers back to this relation's attnums."
  (let ((candidates
          (cons (make-select-scan-path :kind :seq-scan)
                (loop for qual in (where-conjuncts where)
                      append (index-paths-for-equality
                              table qual column-offset)))))
    (choose-cheapest-select-path table candidates)))

(defun select-relation-columns (relation)
  (loop for column across (pg-class-columns (select-relation-table relation))
        collect (let* ((attnum (pg-column-attnum column))
                       (expr (list :col (+ (select-relation-offset relation)
                                           attnum)
                                   :type (pg-column-type column)
                                   :typmod (pg-column-typmod column)
                                   :table-oid
                                   (pg-class-oid (select-relation-table relation))
                                   :attnum attnum)))
                  (make-select-column
                   :name (pg-column-name column)
                   :expr expr
                   :type (pg-column-type column)
                   :typmod (pg-column-typmod column)
                   :table-oid (pg-class-oid (select-relation-table relation))
                   :attnum attnum))))

(defun make-base-select-scope (name alias table)
  (let* ((width (length (pg-class-columns table)))
         (relation (make-select-relation :name name :alias alias
                                         :table table :offset 0 :width width)))
    (make-select-scope :relations (list relation)
                       :columns (select-relation-columns relation)
                       :width width)))

(defun extend-select-scope (scope relation)
  (let* ((qualifier (select-relation-qualifier relation))
         (duplicate
           (find qualifier (select-scope-relations scope)
                 :key #'select-relation-qualifier :test #'string=)))
    (when duplicate
      (ereport "42712" "table name \"~a\" specified more than once"
               qualifier))
    (make-select-scope
     :relations (append (select-scope-relations scope) (list relation))
     :columns (append (select-scope-columns scope)
                      (select-relation-columns relation))
     :width (+ (select-scope-width scope) (select-relation-width relation)))))

(defun visible-column-by-name (columns name)
  (let ((matches (remove-if-not
                  (lambda (column)
                    (string= name (select-column-name column)))
                  columns)))
    (cond
      ((null matches)
       (ereport "42703" "column \"~a\" specified in USING clause does not exist"
                name))
      ((rest matches)
       (ereport "42702" "column reference \"~a\" is ambiguous" name))
      (t (first matches)))))

(defun common-using-type (left-type right-type name)
  (cond
    ((eq left-type right-type) left-type)
    ((and (member left-type '(:int4 :int8))
          (member right-type '(:int4 :int8)))
     :int8)
    (t
     (ereport "42804" "JOIN/USING types ~a and ~a cannot be matched for column ~a"
              left-type right-type name))))

(defun common-using-typmod (left right common-type)
  (if (and (eq (select-column-type left) common-type)
           (eq (select-column-type right) common-type)
           (= (select-column-typmod left) (select-column-typmod right)))
      (select-column-typmod left)
      -1))

(defun using-column-expression (column type typmod)
  (let ((expr (select-column-expr column)))
    (if (and (eq (select-column-type column) type)
             (= (select-column-typmod column) typmod))
        expr
        (list :join-relabel expr :type type :typmod typmod))))

(defun using-column-pairs (left-columns right-columns names)
  (when (null names)
    (ereport "42601" "USING requires at least one column"))
  (let ((seen '()))
    (loop for name in names
          collect
          (progn
            (when (member name seen :test #'string=)
              (ereport "42701" "column \"~a\" specified more than once in USING clause"
                       name))
            (push name seen)
            (let* ((left (visible-column-by-name left-columns name))
                   (right (visible-column-by-name right-columns name))
                   (type (common-using-type (select-column-type left)
                                            (select-column-type right)
                                            name)))
              (list :left left :right right :type type
                    :typmod (common-using-typmod left right type)))))))

(defun using-join-qual (pairs)
  (let ((equalities
          (mapcar (lambda (pair)
                    (list :binop "="
                          (select-column-expr (getf pair :left))
                          (select-column-expr (getf pair :right))
                          :type :bool :typmod -1))
                  pairs)))
    (reduce (lambda (left right)
              (list :and left right :type :bool :typmod -1))
            equalities)))

(defun using-output-column (name pair join-type)
  (let* ((left (getf pair :left))
         (right (getf pair :right))
         (type (getf pair :type))
         (typmod (getf pair :typmod))
         ;; C's buildMergedJoinVar relabels only inputs whose type or typmod
         ;; differs from the common output. Keep plain Vars plain so origin
         ;; metadata remains available when the selected side needs no cast.
         (left-expr (using-column-expression left type typmod))
         (right-expr (using-column-expression right type typmod))
         (expr (case join-type
                 (:right right-expr)
                 (:full (list :join-coalesce left-expr right-expr
                              :type type :typmod typmod))
                 (:inner (cond ((eq (first left-expr) :col) left-expr)
                               ((eq (first right-expr) :col) right-expr)
                               (t left-expr)))
                 (t left-expr))))
    (make-select-column
     :name name :expr expr :type type :typmod typmod
     ;; PostgreSQL does not mark a merged JOIN USING alias as a direct
     ;; reference unless the selected expression remains a plain base Var.
     :table-oid (if (eq (first expr) :col) (or (getf expr :table-oid) 0) 0)
     :attnum (if (eq (first expr) :col)
                 (or (getf expr :attnum) (second expr))
                 0))))

(defun using-output-columns (left-columns right-columns names pairs join-type)
  (append
   (loop for name in names
         for pair in pairs
         collect (using-output-column name pair join-type))
   (remove-if (lambda (column)
                (member (select-column-name column) names :test #'string=))
              left-columns)
   (remove-if (lambda (column)
                (member (select-column-name column) names :test #'string=))
              right-columns)))

(defun select-table-or-error (name)
  (let ((table (or (when (member name '("pg_replication_slots"
                                        "pg_catalog.pg_replication_slots")
                                 :test #'string=)
                     (make-pg-class
                      :oid #x7FFFFFFE :name "pg_replication_slots"
                      :relfilenode #x7FFFFFFD
                      :columns
                      (vector (make-pg-column :name "slot_name" :type :varchar
                                              :typmod -1 :attnum 1 :notnull t)
                              (make-pg-column :name "active" :type :bool
                                              :typmod -1 :attnum 2 :notnull t)
                              (make-pg-column :name "restart_lsn" :type :varchar
                                              :typmod -1 :attnum 3 :notnull t)
                              (make-pg-column :name "confirmed_flush_lsn"
                                              :type :varchar :typmod -1
                                              :attnum 4 :notnull nil)
                              (make-pg-column :name "retained_wal_bytes"
                                              :type :int8 :typmod -1
                                              :attnum 5 :notnull t)
                              (make-pg-column :name "wal_status" :type :varchar
                                              :typmod -1 :attnum 6 :notnull t))))
                   (lookup-table name))))
    (unless table
      (ereport "42P01" "relation \"~a\" does not exist" name))
    table))

(defun select-virtual-row-source (table)
  (when (string= (pg-class-name table) "pg_replication_slots")
    #'wal-replication-slot-rows))

(defun make-select-relation-spec (name alias table offset)
  (let ((row-source (select-virtual-row-source table)))
    (list :name name :alias alias :table table
          :columns (pg-class-columns table)
          :offset offset
          :width (length (pg-class-columns table))
          :virtual-p (not (null row-source))
          :row-source row-source)))

(defun prepare-select (ast)
  ;; Resolve all names on one catalog view. Reader registration below protects
  ;; that view from DDL before any heap or index page is scanned.
  (with-write-lock
    (let* ((from-name (getf ast :from))
           (from-alias (getf ast :from-alias))
           (first-table (when from-name (select-table-or-error from-name)))
           (scope (when first-table
                    (make-base-select-scope from-name from-alias first-table)))
           (relations (when scope
                        (list (make-select-relation-spec
                               from-name from-alias first-table 0))))
           (joins '()))
      (dolist (join-ast (getf ast :joins))
        (let* ((name (getf join-ast :table))
               (alias (getf join-ast :alias))
               (table (select-table-or-error name))
               (offset (select-scope-width scope))
               (relation (make-select-relation
                          :name name :alias alias :table table :offset offset
                          :width (length (pg-class-columns table))))
               (right-columns (select-relation-columns relation))
               (join-type (getf join-ast :type))
               (using-names (and (getf join-ast :using)
                                 (getf join-ast :condition)))
               (pairs (when using-names
                        (using-column-pairs (select-scope-columns scope)
                                            right-columns using-names)))
               (join-scope (extend-select-scope scope relation))
               (qual (cond
                       ((eq join-type :cross) '(:bool t))
                       (using-names (using-join-qual pairs))
                       (t (analyze-expr (getf join-ast :condition)
                                        join-scope))))
               (columns (if using-names
                            (using-output-columns
                             (select-scope-columns scope) right-columns
                             using-names pairs join-type)
                            (select-scope-columns join-scope))))
          (setf scope (make-select-scope
                       :relations (select-scope-relations join-scope)
                       :columns columns
                       :width (select-scope-width join-scope)))
          (push (list :type join-type :qual qual :left-width offset
                      :right-width (select-relation-width relation))
                joins)
          (setf relations
                (append relations
                        (list (make-select-relation-spec
                               name alias table offset))))))
      (setf joins (nreverse joins))
      (let* ((where (when (getf ast :where)
                      (analyze-expr (getf ast :where) scope)))
             (aggregate-p
               (or (getf ast :group-by)
                   (some (lambda (target)
                           (eq (first (getf target :expr)) :count-star))
                         (getf ast :targets))))
             (group-keys (when (getf ast :group-by)
                           (analyze-group-keys (getf ast :group-by) scope)))
             (targets (if aggregate-p
                          (analyze-aggregate-targets
                           (getf ast :targets) scope group-keys)
                          (analyze-targets (getf ast :targets) scope)))
             (aggregate-count
               (if aggregate-p
                   (count-if (lambda (target)
                               (getf target :aggregate-index))
                             targets)
                   0))
             (order (if aggregate-p
                        (analyze-aggregate-order
                         (getf ast :order-by) scope targets group-keys)
                        (analyze-order (getf ast :order-by) scope targets)))
             (limit (getf ast :limit)))
        (list :table first-table
              :relations relations
              :scope scope
              :targets targets
              :where where
              :group-keys group-keys
              :aggregate-p aggregate-p
              :aggregate-count aggregate-count
              :order order
              :limit limit
              :join-order nil
              :joins joins)))))
(defun select-index-current-p (table idx)
  (and (= (pg-index-table-oid idx) (pg-class-oid table))
       (member idx (table-indexes table) :test #'eq)))

(defun select-relation-reader-relfilenodes (relations)
  (mapcan (lambda (spec)
            (when (not (getf spec :virtual-p))
              (let* ((table (getf spec :table))
                     (path (getf spec :path))
                     (index (and (eq (select-scan-path-kind path) :index-scan)
                                 (select-scan-path-index path))))
                (cons (pg-class-relfilenode table)
                      (when index
                        (list (pg-index-relfilenode index)))))))
          relations))

(defmacro with-registered-select-relations ((registered-relations relations)
                                            &body body)
  "Protect every relation and access path before evaluating BODY.

All heap/index reader registrations happen before the catalog mutex is taken
to revalidate the planned table and index identities.  This keeps the
registration-to-DDL handoff free of a lock-order inversion."
  (let ((relation-list (gensym "RELATIONS-"))
        (xid (gensym "XID-"))
        (spec (gensym "SPEC-"))
        (table (gensym "TABLE-"))
        (path (gensym "PATH-"))
        (index (gensym "INDEX-"))
        (registered-relfilenodes (gensym "REGISTERED-RELFILENODES-")))
    `(let ((,relation-list (mapcar #'copy-list ,relations)))
       (with-registered-relation-access
           (,registered-relfilenodes ,xid :read)
           (select-relation-reader-relfilenodes ,relation-list)
           (dolist (,spec ,relation-list)
             (unless (getf ,spec :virtual-p)
               (let* ((,table (getf ,spec :table))
                      (,path (getf ,spec :path))
                      (,index (and (eq (select-scan-path-kind ,path) :index-scan)
                                   (select-scan-path-index ,path))))
                 (ensure-current-table ,table)
                 (unless (eq (getf ,spec :columns) (pg-class-columns ,table))
                   (ereport "0A000"
                            "table definition changed during query planning"))
                 (when (and ,index (not (select-index-current-p ,table ,index)))
                   ;; DDL completed after planning. The old index reader remains
                   ;; registered until transaction end; use SeqScan for this query.
                   (setf (getf ,spec :path)
                         (make-select-scan-path :kind :seq-scan))))))
         (let ((,registered-relations ,relation-list))
           ,@body)))))

(defun select-scan-index-key (table path environment)
  (let ((parameter (select-scan-path-parameter-expr path)))
    (if (null parameter)
        (select-scan-path-key path)
        (let* ((value (eval-expr parameter environment))
               (types (index-types (select-scan-path-index path) table)))
          (when (indexable-equality-value-p value (first types))
            (encode-index-key (list value) types))))))

(defun scan-select-index (table path snapshot callback &optional environment)
  (let* ((idx (select-scan-path-index path))
         (relfilenode (pg-class-relfilenode table))
         (key (select-scan-index-key table path environment)))
    (when key
      ;; B-tree synchronization stays scoped to this index. The cursor streams
      ;; one TID to the executor, so LIMIT can stop before later duplicates.
      ;; Parameter values are evaluated for each nested-loop outer row, matching
      ;; PostgreSQL's ExecIndexScan/ScanKey setup in nodeIndexscan.c.
      (btree-scan-equal
       (pg-index-relfilenode idx) key
       (lambda (tid)
         (check-for-interrupts)
         (let ((tuple (heapam-get-tuple relfilenode (car tid) (cdr tid)
                                        (pg-class-columns table))))
           (when (and tuple (htup-visible-p tuple snapshot))
             (funcall callback tuple))))))))

(defun scan-select-path (table relfilenode path snapshot callback
                         &optional environment)
  (ecase (select-scan-path-kind path)
    (:seq-scan
     (heapam-scan relfilenode (pg-class-columns table) snapshot callback))
    (:index-scan
     (scan-select-index table path snapshot callback environment))))

(defun scan-select-relation (relation path snapshot callback
                             &optional environment)
  "Scan a registered relation, including executor-backed virtual catalogs."
  (let ((row-source (getf relation :row-source))
        (table (getf relation :table)))
    (if row-source
        (dolist (row (funcall row-source))
          (check-for-interrupts)
          (when (eq (funcall callback row) :stop)
            (return-from scan-select-relation :stop)))
        (scan-select-path table (pg-class-relfilenode table) path snapshot
                          callback environment))))

(defun make-select-relation-environment (relations relation row)
  "Place ROW in its source-order slots after a greedy join-order scan."
  (let* ((width (reduce #'+ relations :key (lambda (spec) (getf spec :width))
                                      :initial-value 0))
         (environment (make-array width :initial-element +null+)))
    (replace environment row :start1 (getf relation :offset))
    environment))

(defun make-select-result-row (environment where targets)
  "Apply WHERE before projecting ENVIRONMENT, retaining it for ORDER BY."
  (when (or (null where) (true-p (eval-expr where environment)))
    (cons (map 'vector
               (lambda (target)
                 (eval-expr (getf target :expr) environment))
               targets)
          environment)))

(defun collect-select-no-from-rows (where targets)
  (let ((row (make-select-result-row nil where targets)))
    (if row (list row) nil)))

(defun collect-select-table-rows (table relfilenode path where targets)
  (let ((rows '()))
    (labels ((consider (tuple)
               (check-for-interrupts)
               (let ((env (htup-values tuple)))
                 ;; Index quals only produce candidates.  Recheck the complete
                 ;; WHERE expression before projection just like SeqScan.
                 (let ((row (make-select-result-row env where targets)))
                   (when row (push row rows))))))
      (scan-select-path table relfilenode path *current-snapshot* #'consider))
    (nreverse rows)))

(defun combine-select-join-environment (left right join)
  (if (getf join :reordered)
      (let* ((offset (getf join :right-offset))
             (environment (copy-seq left)))
        (replace environment right :start1 offset)
        environment)
      (concatenate 'vector left right)))

(defun stream-select-join-environments (left-stream right-stream join callback)
  "Run a lazy nested-loop join with a spillable, replayable inner stream.

The inner stream is opened only after the first outer row arrives. Its rows are
cached as they are consumed, so the first joined row can be returned before the
inner input is exhausted and later outer rows can replay the same input."
  (let* ((join-type (getf join :type))
         (qual (getf join :qual))
         (left-width (getf join :left-width))
         (right-width (getf join :right-width))
         (null-left (make-array left-width :initial-element +null+))
         (null-right (make-array right-width :initial-element +null+))
         (right/full-p (member join-type '(:right :full)))
         (work-memory (max 1 *query-work-memory-bytes*))
         (spool-memory (if right/full-p
                           (max 1 (floor work-memory 2))
                           work-memory))
         (match-memory (max 1 (- work-memory spool-memory))))
    (with-query-temp-files ()
                           (with-query-row-spool (right-spool spool-memory)
                             (labels ((run-join (matched-right)
                                        (let ((right-loaded nil)
                                              (outer-seen nil)
                                              (stopped nil))
                                          (labels ((emit (environment)
                                                     (when (eq (funcall callback environment) :stop)
                                                       (setf stopped t)
                                                       :stop))
                                                   (process-right (left right right-index matched-left)
                                                     (check-for-interrupts)
                                                     (let ((joined
                                                             (combine-select-join-environment
                                                              left right join)))
                                                       (when (true-p (eval-expr qual joined))
                                                         ;; Outer-join match state comes from ON
                                                         ;; alone; WHERE remains above this node.
                                                         (setf (car matched-left) t)
                                                         (when matched-right
                                                           (query-match-map-mark
                                                            matched-right right-index))
                                                         (emit joined))))
                                                   (load-right-for-left (left matched-left)
                                                     (setf right-loaded t)
                                                     (funcall
                                                      right-stream
                                                      (lambda (right)
                                                        (let ((right-index
                                                                (query-row-spool-append
                                                                 right-spool right)))
                                                          (when matched-right
                                                            (let ((map-index
                                                                    (query-match-map-append
                                                                     matched-right)))
                                                              (unless (= right-index map-index)
                                                                (error "Join spool and match map lost row alignment"))))
                                                          (process-right left right right-index
                                                                         matched-left))))
                                                     (unless stopped
                                                       (query-row-spool-seal right-spool)))
                                                   (visit-left (left)
                                                     (unless stopped
                                                       (check-for-interrupts)
                                                       (setf outer-seen t)
                                                       (let ((matched-left (list nil)))
                                                         (if right-loaded
                                                             (map-query-row-spool
                                                              right-spool
                                                              (lambda (right right-index)
                                                                (process-right
                                                                 left right right-index
                                                                 matched-left)))
                                                             (load-right-for-left left matched-left))
                                                         (when (and (not stopped)
                                                                    (not (car matched-left))
                                                                    (member join-type '(:left :full)))
                                                           (emit (concatenate 'vector
                                                                              left null-right))))
                                                       (when stopped :stop)))
                                                   (emit-unmatched-rights ()
                                                     (if outer-seen
                                                         (progn
                                                           (query-match-map-finish matched-right)
                                                           (map-query-row-spool
                                                            right-spool
                                                            (lambda (right right-index)
                                                              (unless
                                                                  (query-match-map-matched-p
                                                                   matched-right right-index)
                                                                (emit (concatenate 'vector
                                                                                   null-left right))))))
                                                         ;; RIGHT/FULL JOIN with an empty outer
                                                         ;; input can stream the inner rows directly.
                                                         (funcall
                                                          right-stream
                                                          (lambda (right)
                                                            (emit (concatenate 'vector
                                                                               null-left right)))))))
                                            (funcall left-stream #'visit-left)
                                            (unless stopped
                                              (when right/full-p
                                                (emit-unmatched-rights)))
                                            (when stopped :stop)))))
                               (if right/full-p
                                   (with-query-match-map (matched-right match-memory)
                                     (run-join matched-right))
                                   (run-join nil)))))))

(defun stream-select-indexed-join-environments
    (left-stream parameterized-right-stream join callback)
  "Run an INNER/LEFT nested loop with an index scan keyed by each outer row.

Unlike the replayable SeqScan inner used by stream-select-join-environments,
the right input changes with every outer row and must not be spooled once."
  (let* ((join-type (getf join :type))
         (qual (getf join :qual))
         (right-width (getf join :right-width))
         (null-right (make-array right-width :initial-element +null+))
         (stopped nil))
    (unless (member join-type '(:inner :left))
      (error "Parameterized index join does not support ~a joins" join-type))
    (labels ((emit (environment)
               (when (eq (funcall callback environment) :stop)
                 (setf stopped t)
                 :stop))
             (visit-left (left)
               (unless stopped
                 (check-for-interrupts)
                 (let ((matched nil))
                   (funcall
                    parameterized-right-stream left
                    (lambda (right)
                      (let ((joined
                              (combine-select-join-environment left right join)))
                        (when (true-p (eval-expr qual joined))
                          (setf matched t)
                          (emit joined)))))
                   (when (and (not stopped) (not matched)
                              (eq join-type :left))
                     (emit (concatenate 'vector left null-right))))
                 (when stopped :stop))))
      (funcall left-stream #'visit-left)
      (if stopped :stop (values)))))

(defun stream-select-joined-environments (relations joins callback)
  (labels ((stream-level (level level-callback)
             (if (= level 1)
                 (let* ((relation (first relations))
                        (table (getf relation :table)))
                   (scan-select-path
                    table (pg-class-relfilenode table) (getf relation :path)
                    *current-snapshot*
                    (lambda (tuple)
                      (check-for-interrupts)
                      (funcall level-callback (htup-values tuple)))))
                 (let* ((relation (nth (1- level) relations))
                        (join (nth (- level 2) joins))
                        (table (getf relation :table))
                        (path (getf relation :path)))
                   ;; Joined WHERE clauses stay above the join tree. In
                   ;; particular, filtering the right input here could turn a
                   ;; matched row into a fabricated NULL-extended row.
                   (unless (eq (select-scan-path-kind path) :seq-scan)
                     (ereport "XX000" "joined relation scan must use SeqScan"))
                   (stream-select-join-environments
                    (lambda (left-callback)
                      (stream-level (1- level) left-callback))
                    (lambda (right-callback)
                      (scan-select-path
                       table (pg-class-relfilenode table) path
                       *current-snapshot*
                       (lambda (tuple)
                         (check-for-interrupts)
                         (funcall right-callback (htup-values tuple)))))
                    join level-callback)))))
    (when relations
      (stream-level (length relations) callback))))

(defun stream-select-joined-rows (relations joins where targets callback)
  (stream-select-joined-environments
   relations joins
   (lambda (environment)
     (check-for-interrupts)
     (let ((row (make-select-result-row environment where targets)))
       (when row (funcall callback row))))))

(defun collect-select-joined-rows (relations joins where targets)
  (let ((rows '()))
    (stream-select-joined-rows
     relations joins where targets
     (lambda (row)
       (push row rows)))
    (nreverse rows)))

(defun send-select-row (stream row targets)
  (send-data-row-text
   stream
   (loop for value across (car row)
         for target in targets
         collect (value->text value (getf target :type)))))

(defun select-path-description (table path)
  (case (select-scan-path-kind path)
    (:index-scan
     (format nil "Index Scan using ~a on ~a"
             (pg-index-name (select-scan-path-index path))
             (pg-class-name table)))
    (:seq-scan
     (format nil "Seq Scan on ~a" (pg-class-name table)))
    (:result "Result")))
