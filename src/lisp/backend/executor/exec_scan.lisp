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

MODE is the literal :READ or :WRITE.  Readers register the complete set
atomically and use the reader identity helper, which preserves an existing
virtual ID across a role change.  Writers must register exactly one relation."
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
            (,xid ,(if (eq mode :read)
                       '(get-current-reader-xid)
                       '(get-current-xid))))
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
                        :attnum (sql-result-column-attnum
                                 (getf target :table-oid) (getf target :attnum))
                        :type-oid oid
                        :typlen (sql-expression-type-length (getf target :type))
                        :typmod (getf target :typmod)
                        :format 0)))

(defun order-compare (a b)
  (cond
    ((and (eq a +null+) (eq b +null+)) 0)
    ((eq a +null+) 1)
    ((eq b +null+) -1)
    (t (compare-3way a b))))

(defun select-order-value (expression row)
  ;; Reuse selected projection slots across ordinary, top-N and external sorts.
  (if (eq (first expression) :sort-output)
      (aref (car row) (1- (second expression)))
      (eval-expr expression (cdr row))))

(defun row-less-p (a b order)
  (loop for (expr . desc) in order
        do (let* ((va (select-order-value expr a))
                  (vb (select-order-value expr b))
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
                             (select-order-value (car entry) row))
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
  "Check whether VALUE has a supported representation for an equality index probe."
  (and (not (eq value +null+))
       (case type
         ((:int2 :int4 :int8 :date :time :timestamp :timestamptz) (integerp value))
         (:numeric (sql-numeric-p value))
         (:interval (sql-interval-p value))
         (:timetz (sql-timetz-p value))
         (:float4 (typep value 'single-float))
         (:float8 (typep value 'double-float))
         ((:varchar :text) (stringp value))
         (:bytea (typep value '(vector (unsigned-byte 8))))
         (:jsonb (sql-jsonb-p value))
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
  "Use ANALYZE's type-consistent MCV frequencies and residual estimate.

PostgreSQL's selfuncs.c equality estimator applies the query operator when it
compares a constant with MCV values. Reuse ANALYZE's equality helper so fresh
BYTEA vectors match the byte-content values stored in the MCV list."
  (let ((statistics (table-statistics-column table attnum)))
    (if (null statistics)
        0.1d0
        (let* ((mcv (pg-column-statistics-most-common-values statistics))
               (common (and value
                            (assoc value mcv
                                   :test #'analyze-statistics-value-equal-p)))
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
              (when (if constant-p
                        (indexable-equality-value-p value type)
                        (case type
                          ((:int2 :int4 :int8)
                           (member (expr-type parameter-expr) '(:int2 :int4 :int8)))
                          ((:date :time :timestamp :timestamptz :interval :timetz) (eq (expr-type parameter-expr) type))
                          (:numeric
                           (member (expr-type parameter-expr) '(:int2 :int4 :int8 :numeric)))
                          ((:float4 :float8) (eq type (expr-type parameter-expr)))
                          (otherwise t)))
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
  (when (and (select-relation-origins relation)
             (/= (length (select-relation-origins relation))
                 (length (pg-class-columns (select-relation-table relation)))))
    (error "Derived column origins do not match projected columns"))
  (loop for column across (pg-class-columns (select-relation-table relation))
        for origin in (or (select-relation-origins relation)
                          (make-list (select-relation-width relation)))
        for slot = (pg-column-attnum column)
        for table-oid = (if origin (getf origin :table-oid)
                            (pg-class-oid (select-relation-table relation)))
        for attnum = (if origin (getf origin :attnum) slot)
        for expr = (list :col (+ (select-relation-offset relation) slot)
                         :type (pg-column-type column) :typmod (pg-column-typmod column)
                         :notnull (pg-column-notnull column)
                         :table-oid table-oid :attnum attnum)
        collect (make-select-column
                 :name (pg-column-name column) :expr expr
                 :type (pg-column-type column) :typmod (pg-column-typmod column)
                 :table-oid table-oid :attnum attnum)))

(defun select-relation-initial-scope (relation)
  "Construct the first FROM namespace from one relation's columns and width."
  (make-select-scope :relations (list relation)
                     :columns (select-relation-columns relation)
                     :width (select-relation-width relation)))

(defun make-base-select-scope (name alias table)
  (select-relation-initial-scope
   (make-select-relation :name (sql-relation-base-name name) :alias alias
                         :table table :offset 0
                         :width (length (pg-class-columns table)))))

(defun extend-select-scope (scope relation)
  (let* ((qualifier (select-relation-qualifier relation))
         (duplicate
           (and qualifier
                (find qualifier (select-scope-relations scope)
                      :key #'select-relation-qualifier
                      :test (lambda (a b) (and b (string= a b)))))))
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
    ((and (member left-type '(:int2 :int4 :int8 :numeric :float4 :float8))
          (member right-type '(:int2 :int4 :int8 :numeric :float4 :float8))
          (or (member left-type '(:float4 :float8)) (member right-type '(:float4 :float8))))
     (if (or (eq left-type :float8) (eq right-type :float8)) :float8 :float4))
    ((and (member left-type '(:int2 :int4 :int8 :numeric))
          (member right-type '(:int2 :int4 :int8 :numeric))
          (or (eq left-type :numeric) (eq right-type :numeric)))
     :numeric)
    ((and (member left-type '(:int2 :int4 :int8))
          (member right-type '(:int2 :int4 :int8)))
     (if (or (eq left-type :int8) (eq right-type :int8)) :int8 :int4))
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
        (if (and (member type '(:float4 :float8)) (not (eq (select-column-type column) type)))
            (list :cast expr :type type :typmod typmod)
            (list :join-relabel expr :type type :typmod typmod)))))

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
                    (let ((operands (analyze-comparison-operands
                                     (select-column-expr (getf pair :left))
                                     (select-column-expr (getf pair :right)))))
                      (list :binop "=" (first operands) (second operands) :type :bool :typmod -1)))
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
  (sql-relation-or-error name))

(defun select-virtual-row-source (table)
  (sql-system-row-source table))

(defun make-select-relation-spec (name alias table offset)
  (let ((row-source (select-virtual-row-source table)))
    (list :name (sql-relation-base-name name) :alias alias :table table
          :columns (pg-class-columns table)
          :offset offset
          :width (length (pg-class-columns table))
          :virtual-p (not (null row-source))
          :row-source row-source)))

(defun prepared-query-lateral-reference-p (prepared)
  ;; Include references in unused CTEs: join legality is an analysis property,
  ;; even when the referenced producer will never execute.
  (let ((found nil))
    (map-prepared-query-expressions
     (lambda (form depth)
       (when (and (consp form) (eq (first form) :outer-ref)
                  (= (second form) (1+ depth)))
         (setf found t))
       form)
     prepared)
    found))

(defun prepare-select-source (source alias offset &optional preceding-scope)
  "Prepare a FROM namespace entry; callers hold the catalog view lock."
  (let ((definition (cte-source-definition source)))
    (when definition
      (return-from prepare-select-source (make-cte-relation-spec definition alias offset))))
  (if (sql-derived-relation-p source)
      (let* ((query (sql-derived-relation-query source))
             (prepared (prepare-select-internal query
                                                (if (getf query :lateral)
                                                    (let ((scope (copy-select-scope (or preceding-scope (make-select-scope)))))
                                                      ;; At the source's own lexical level, retain the target as a
                                                      ;; forbidden namespace entry for C-compatible 42P10 errors.
                                                      ;; Nested scopes still resolve their own names first.
                                                      (when (and *dml-source-target-scope*
                                                                 (= (car *dml-source-target-scope*) *select-cte-level*))
                                                        (setf (select-scope-relations scope)
                                                              (append
                                                               (mapcar (lambda (relation)
                                                                         (let ((copy (copy-select-relation relation)))
                                                                           (setf (select-relation-lateral-forbidden-p copy) t)
                                                                           copy))
                                                                       (select-scope-relations (cdr *dml-source-target-scope*)))
                                                               (select-scope-relations scope))))
                                                      scope)
                                                    (make-select-scope))))
             (targets (let ((targets (getf prepared :targets))
                            (aliases (getf query :column-aliases)))
                        (when (> (length aliases) (length targets))
                          (ereport "42P10" "table ~s has ~d columns available but ~d columns specified"
                                   alias (length targets) (length aliases)))
                        (loop for target in targets for index from 0
                              collect (if (< index (length aliases))
                                          (let ((copy (copy-list target)))
                                            (setf (getf copy :name) (nth index aliases))
                                            copy)
                                          target))))
             (table (make-pg-class
                     :oid 0 :name (or alias "*SELECT*")
                     :columns (coerce
                               (loop for target in targets for slot from 1
                                     collect (make-pg-column
                                              :name (getf target :name)
                                              :type (getf target :type)
                                              :typmod (getf target :typmod)
                                              :attnum slot))
                               'vector))))
        (list :name alias :alias alias :table table :offset offset
              :width (length targets) :columns (pg-class-columns table)
              :virtual-p t :origins targets :subquery-prepared prepared
              ;; Legality includes unused CTEs; runtime dependencies need only
              ;; the outer frames reached by executable expressions.
              :lateral-reference (and (getf query :lateral)
                                      (prepared-query-lateral-reference-p prepared))
              :lateral-dependent (and (getf query :lateral)
                                      (member 1 (prepared-query-outer-depths prepared)))
              :subquery-plan nil :subquery-logical nil :subquery-physical nil))
      (make-select-relation-spec source alias (select-table-or-error source) offset)))

(defun select-source-scope-relation (source)
  (make-select-relation :name (getf source :name) :alias (getf source :alias)
                        :table (getf source :table) :origins (getf source :origins)
                        :offset (getf source :offset) :width (getf source :width)))

(defun prepare-select (ast)
  "Resolve the complete nested query under one catalog view."
  ;; Clear transient analysis frames only after all nested ownership is resolved.
  (with-write-lock (clear-query-analysis-contexts (prepare-select-internal ast))))

(defun prepare-union-select (ast)
  "Prepare both arms under the caller's catalog view."
  (let* ((*select-cte-level* (1- *select-cte-level*))
         (definition (and *select-recursive-definition*
                          (eq ast (sql-cte-query *select-recursive-definition*))
                          *select-recursive-definition*))
         (left (prepare-select-internal (getf ast :left)))
         (frozen (when definition
                   (freeze-recursive-cte-targets (getf left :targets) (sql-cte-aliases definition))))
         (right (progn
                  (when definition
                    (setf (sql-cte-prepared definition)
                          (list :targets frozen :recursive-cte definition :recursive-placeholder t)))
                  (let ((*select-recursive-worktables*
                          (if definition (cons definition *select-recursive-worktables*)
                              *select-recursive-worktables*)))
                    (prepare-select-internal (getf ast :right))))))
    (when definition (validate-recursive-term-aggregates right))
    (multiple-value-bind (targets coercions)
        (analyze-union-targets (or frozen (getf left :targets)) (getf right :targets))
      (when (and (not (eq (getf ast :set-operation) :union-all))
                 (some (lambda (target) (eq (getf target :type) :json)) targets))
        (ereport "42883" "could not identify an equality operator for type json"))
      (when definition
        (loop for seed in frozen for target in targets for position from 1
              unless (and (eq (getf seed :type) (getf target :type))
                          (eql (getf seed :typmod) (getf target :typmod)))
                do (ereport "42804" "recursive query ~s column ~d has a different type or typmod overall"
                            (sql-cte-name definition) position)))
      (list :set-operation (getf ast :set-operation) :left left :right right :coercions coercions
            :recursive-cte definition
            :recursive-union-all (eq (getf ast :set-operation) :union-all)
            :targets targets :relations nil
            :order (analyze-union-order (getf ast :order-by) targets)
            :limit (getf ast :limit)))))

(defun prepare-select-internal (ast &optional (parent *select-outer-scope*))
  (let* ((*select-outer-scope* parent)
         (*select-cte-environment* *select-cte-environment*)
         (*select-cte-level* (1+ *select-cte-level*))
         (*select-sublinks* nil)
         (*select-window-functions* (make-array 0 :adjustable t :fill-pointer 0))
         (*select-window-specifications* (make-array 0 :adjustable t :fill-pointer 0))
         (*select-named-windows* (prepare-named-windows (getf ast :windows)))
         (*inside-window-call-p* nil)
         (*window-analysis-targets* nil)
         (*window-key-targets* nil)
         (*defer-group-rewrite-p* t)
         (*select-sublinks-allowed-p* t)
         (*select-analysis-frame*
           (list :parent (and parent (select-scope-analysis-frame parent))
                 :descriptors (make-array 0 :adjustable t :fill-pointer 0)
                 :group-keys nil :kind :from :inside-input nil))
         (*select-aggregate-analysis* *select-analysis-frame*)
         (*inside-aggregate-input-p* nil)
         (*aggregate-order-mode* nil)
         (*select-set-functions-allowed-p* nil)
         (definitions (prepare-with-definitions (getf ast :with) (getf ast :with-recursive)))
         (prepared (%prepare-select-internal ast)))
    (setf (getf prepared :ctes) definitions
          (getf prepared :deferred-window-folding) *cte-definition-analysis-p*)
    (setf (getf prepared :sublinks) (nreverse *select-sublinks*))
    prepared))

(defun having-immutable-function-p (name)
  ;; Match the immutable declarations in pg_proc.dat for supported calls.
  ;; Other calls remain opaque even when their arguments are constants.
  (or (integer-catalog-alias-p name)
      (float-catalog-alias name)
      (trig-function-p name)
      (numeric-arithmetic-function-p name)
      (numeric-helper-function-p name)
      (member name '("json_typeof" "jsonb_typeof" "json_array_length" "jsonb_array_length"
                     "lower" "substr" "substring" "length" "char_length" "character_length" "octet_length"
                     "pg_wal_lsn_diff" "pg_walfile_name"
                     "abs" "numeric_abs" "sign" "round" "trunc" "ceil" "ceiling" "floor"
                     "sqrt" "numeric_sqrt" "exp" "numeric_exp" "ln" "numeric_ln"
                     "log" "log10" "numeric_log" "power" "pow" "numeric_power" "dpow"
                     "scale" "min_scale" "trim_scale" "width_bucket")
              :test #'string=)))

(defun having-literal-expression-p (expression)
  ;; Gate folding accepts only literal inputs and immutable operators/functions.
  ;; STABLE functions such as current_setting must still run at execution time.
  (and (consp expression)
       (case (first expression)
         ((:const :bool :scalar-array-const) t)
         ((:binop :any :all)
          (and (having-literal-expression-p (third expression))
               (having-literal-expression-p (fourth expression))))
         (:unary (having-literal-expression-p (third expression)))
         ((:cast :not :is-null :is-bool :join-relabel)
          (having-literal-expression-p (second expression)))
         ((:and :or :concat :array-length :join-coalesce)
          (and (having-literal-expression-p (second expression))
               (having-literal-expression-p (third expression))))
         ((:array :coalesce)
          (every #'having-literal-expression-p (second expression)))
         (:case
             (and (every (lambda (branch)
                           (every #'having-literal-expression-p branch))
                         (second expression))
                  (having-literal-expression-p (third expression))))
         (:in
          (and (having-literal-expression-p (second expression))
               (every #'having-literal-expression-p (third expression))))
         (:func
          (and (having-immutable-function-p (second expression))
               (every #'having-literal-expression-p (third expression))))
         (otherwise nil))))

(defun having-gate-skeleton (expression)
  ;; Retain only the immutable expression structure needed for gate folding.
  ;; Opaque leaves own no Vars, scopes or subplan descriptors.  Function argument
  ;; skeletons retain constant errors without ever invoking a nonimmutable call.
  (when (having-literal-expression-p expression)
    (return-from having-gate-skeleton (copy-tree expression)))
  (labels ((children (&rest positions)
             (let ((copy (copy-list expression)))
               (dolist (position positions copy)
                 (setf (nth position copy)
                       (having-gate-skeleton (nth position expression)))))))
    (case (first expression)
      ((:and :or :concat :array-length :join-coalesce) (children 1 2))
      ((:binop :any :all) (children 2 3))
      (:unary (children 2))
      ((:not :is-null :is-bool :cast :join-relabel) (children 1))
      ((:array :coalesce)
       (let ((copy (copy-list expression)))
         (setf (second copy) (mapcar #'having-gate-skeleton (second expression)))
         copy))
      (:case
          (let ((copy (copy-list expression)))
            (setf (second copy)
                  (mapcar (lambda (branch) (mapcar #'having-gate-skeleton branch))
                          (second expression))
                  (third copy) (having-gate-skeleton (third expression)))
            copy))
      (:in
       (let ((copy (children 1)))
         (setf (third copy) (mapcar #'having-gate-skeleton (third expression))
               (getf (cddddr copy) :array-left)
               (when (getf (cddddr expression) :array-left)
                 (having-gate-skeleton (getf (cddddr expression) :array-left)))
               (getf (cddddr copy) :array-items)
               (mapcar #'having-gate-skeleton (getf (cddddr expression) :array-items))
               (getf (cddddr copy) :comparisons)
               (mapcar (lambda (pair) (mapcar #'having-gate-skeleton pair))
                       (getf (cddddr expression) :comparisons)))
         copy))
      (:func
       (if (having-immutable-function-p (second expression))
           (let ((copy (copy-list expression)))
             (setf (third copy) (mapcar #'having-gate-skeleton (third expression)))
             copy)
           (list :having-opaque (mapcar #'having-gate-skeleton (third expression)))))
      (otherwise '(:having-opaque)))))

(defun having-pushdown-safe-p (expression)
  ;; planner.c keeps aggregate, volatile and subplan clauses above grouping.
  (labels ((contains-special (form)
             (and (consp form)
                  (or (subquery-expression-p form)
                      (and (eq (first form) :col) (getf (cddr form) :aggregate-output))
                      (contains-special (car form))
                      (contains-special (cdr form))))))
    (and (not (contains-special expression))
         (not (cte-expression-volatile-p expression)))))

(defun prepare-having-qualifications (where having group-keys scope)
  ;; Validate grouped references before moving clauses back to the input row.
  ;; Without group keys retain the clause above the implicit global group too:
  ;; filtering all input rows must not create an unwanted output group.
  (let ((remaining nil))
    (dolist (clause (where-conjuncts having))
      (let ((rewritten (rewrite-aggregate-columns clause group-keys scope)))
        (if (having-pushdown-safe-p clause)
            (progn
              (let ((input-clause (copy-tree clause)))
                (setf where (if where
                                (list :and where input-clause :type :bool :typmod -1)
                                input-clause)))
              (unless group-keys (push rewritten remaining)))
            (push rewritten remaining))))
    (values where
            (when remaining
              (reduce (lambda (left right) (list :and left right :type :bool :typmod -1))
                      (nreverse remaining))))))

(defun %prepare-select-internal (ast)
  ;; Group rewrites wait for nested analysis; DISTINCT ordering uses output slots.
  (when (getf ast :set-operation)
    (return-from %prepare-select-internal (prepare-union-select ast)))
  ;; Resolve all names on one catalog view. Reader registration below protects
  ;; that view from DDL before any heap or index page is scanned.
  (let* ((from-name (getf ast :from))
         (from-alias (getf ast :from-alias))
         (first-source (when from-name (prepare-select-source from-name from-alias 0)))
         (first-table (getf first-source :table))
         (scope (if first-source
                    (select-relation-initial-scope
                     (select-source-scope-relation first-source))
                    (make-select-scope)))
         (relations (when first-source (list first-source)))
         (joins '()))
    (dolist (join-ast (getf ast :joins))
      (let* ((name (getf join-ast :table))
             (alias (getf join-ast :alias))
             (offset (select-scope-width scope))
             (source (prepare-select-source name alias offset scope))
             (relation (select-source-scope-relation source))
             (right-columns (select-relation-columns relation))
             (join-type (getf join-ast :type))
             (using-names (and (getf join-ast :using)
                               (getf join-ast :condition)))
             (pairs (when using-names
                      (using-column-pairs (select-scope-columns scope)
                                          right-columns using-names)))
             (join-scope (extend-select-scope scope relation))
             (qual (progn
                     (when (and (getf source :lateral-reference)
                                (member join-type '(:right :full)))
                       (ereport "42P10" "the combining JOIN type must be INNER or LEFT for a LATERAL reference"))
                     (setf (getf *select-analysis-frame* :kind) :join-on)
                     (cond
                       ((eq join-type :cross) '(:bool t))
                       (using-names (using-join-qual pairs))
                       (t (coerce-boolean-expression
                           (analyze-expr (getf join-ast :condition) join-scope) "JOIN/ON")))))
             (columns (if using-names
                          (using-output-columns
                           (select-scope-columns scope) right-columns
                           using-names pairs join-type)
                          (select-scope-columns join-scope))))
        (setf scope (make-select-scope
                     :relations (select-scope-relations join-scope)
                     :columns columns
                     :width (select-scope-width join-scope)))
        (push (list :type join-type :qual qual :lateral-dependent (getf source :lateral-dependent)
                    :left-width offset
                    :right-width (select-relation-width relation))
              joins)
        (setf relations
              (append relations
                      (list source)))))
    (setf joins (nreverse joins))
    (let* ((where (progn (setf (getf *select-analysis-frame* :kind) :where)
                         (when (getf ast :where)
                           (coerce-boolean-expression (analyze-expr (getf ast :where) scope) "WHERE"))))
           (where-gate-clauses
             (when (getf ast :having)
               (mapcar #'having-gate-skeleton (where-conjuncts where))))
           (group-keys (progn (setf (getf *select-analysis-frame* :kind) :group-by)
                              (when (getf ast :group-by)
                                (analyze-group-keys (getf ast :group-by) scope))))
           (aggregate-analysis
             (progn
               (setf (getf *select-analysis-frame* :kind) (if (getf ast :from-function) :from-function :select-target)
                     (getf *select-analysis-frame* :group-keys) group-keys)
               (multiple-value-list
                (analyze-aggregate-targets (getf ast :targets) scope group-keys))))
           (targets (progn
                      (setf *window-analysis-targets* (first aggregate-analysis))
                      (resolve-query-window-specifications scope (first aggregate-analysis))
                      (first aggregate-analysis)))
           (aggregates (third aggregate-analysis))
           (having (progn
                     (setf (getf *select-analysis-frame* :kind) :having)
                     (when (getf ast :having)
                       (coerce-boolean-expression
                        (analyze-expr (getf ast :having) scope) "HAVING"))))
           (having-gate-clauses
             (mapcar #'having-gate-skeleton (where-conjuncts having)))
           (aggregate-p (or group-keys having (plusp (length aggregates))))
           (order (progn
                    (setf (getf *select-analysis-frame* :kind) :order-by)
                    (cond
                      ((getf ast :distinct)
                       (analyze-distinct-order
                        (getf ast :order-by) scope targets aggregate-p group-keys aggregates))
                      (aggregate-p
                       (analyze-aggregate-order
                        (getf ast :order-by) scope targets group-keys aggregates t))
                      (t (analyze-order (getf ast :order-by) scope targets)))))
           (aggregate-p (or aggregate-p (plusp (length aggregates))))
           (order (if aggregate-p (finalize-query-group-expressions targets order group-keys scope (getf ast :distinct))
                      order))
           (having-analysis (multiple-value-list
                             (prepare-having-qualifications where having group-keys scope)))
           (where (first having-analysis))
           (having (second having-analysis))
           (width (if aggregate-p (+ (length group-keys) (length aggregates))
                      (select-scope-width scope)))
           (window-analysis (multiple-value-list
                             (finalize-query-windows targets order group-keys scope aggregate-p width)))
           (order (first window-analysis))
           (width (second window-analysis))
           (limit (getf ast :limit))
           (set-analysis
             (multiple-value-list
              (prepare-select-project-set
               targets order
               (+ width (length *select-window-functions*))
               (getf ast :distinct)))))
      (when (and (getf ast :distinct)
                 (some (lambda (target) (eq (getf target :type) :json)) targets))
        (ereport "42883" "could not identify an equality operator for type json"))
      (when (some (lambda (item) (eq (expr-type (car item)) :json))
                  (second set-analysis))
        (ereport "42883" "could not identify an ordering operator for type json"))
      (list :window-validation-keys
            (when *cte-definition-analysis-p*
              (mapcar (lambda (target) (getf target :expr)) *window-key-targets*))
            :window-keys (third window-analysis)
            :window-unused-key-indices (fourth window-analysis)
            :windows (coerce *select-window-functions* 'list) :window-input-width width
            :table first-table
            :source-relations relations
            :relations relations
            :scope scope
            :targets (first set-analysis) :set-functions (third set-analysis) :distinct (getf ast :distinct)
            :where where :having having :has-having (not (null (getf ast :having)))
            ;; Gate skeletons preserve the two preprocessing boundaries after
            ;; pushdown without retaining Vars or subplan descriptors.
            :where-gate-clauses where-gate-clauses
            :having-gate-clauses having-gate-clauses
            :group-keys group-keys
            :aggregate-p aggregate-p
            :aggregate-count (length aggregates) :aggregates (coerce aggregates 'list)
            :order (second set-analysis)
            :limit limit
            :join-order nil
            :joins joins))))

(defun select-index-current-p (table idx)
  (and (= (pg-index-table-oid idx) (pg-class-oid table))
       (member idx (table-indexes table) :test #'eq)))

(defun select-relation-reader-relfilenodes (relations)
  "Capture TOAST identities in RELATIONS and return reader relfilenodes.
Include each non-virtual table, its TOAST heap and index when present, and
its selected scan index.  The captured TOAST identities are revalidated after
reader registration."
  (mapcan (lambda (spec)
            (unless (getf spec :virtual-p)
              (let* ((table (getf spec :table))
                     (toast (pg-class-toast table))
                     (toast-nodes (when toast
                                    (list (pg-toast-relfilenode toast)
                                          (pg-toast-index-relfilenode toast))))
                     (path (getf spec :path))
                     (index (and (eq (select-scan-path-kind path) :index-scan)
                                 (select-scan-path-index path))))
                (unless (loop for key in spec by #'cddr thereis (eq key :registered-toast))
                  (nconc spec (list :registered-toast nil
                                    :registered-toast-nodes nil)))
                (setf (getf spec :registered-toast) toast
                      (getf spec :registered-toast-nodes) toast-nodes)
                (append (list (pg-class-relfilenode table))
                        toast-nodes
                        (when index (list (pg-index-relfilenode index)))))))
          relations))

(defun revalidate-select-relations (relations)
  "Revalidate registered RELATIONS under the caller's write lock.
Signal an error if a table, its columns, or its captured TOAST identity changed.
Replace stale index paths in the copied relation specifications with SeqScans."
  (dolist (spec relations)
    (unless (getf spec :virtual-p)
      (let* ((table (getf spec :table))
             (path (getf spec :path))
             (index (and (eq (select-scan-path-kind path) :index-scan)
                         (select-scan-path-index path))))
        (ensure-current-table table)
        (let ((toast (pg-class-toast table)))
          (unless (and (eq toast (getf spec :registered-toast))
                       (equal (when toast
                                (list (pg-toast-relfilenode toast)
                                      (pg-toast-index-relfilenode toast)))
                              (getf spec :registered-toast-nodes)))
            (ereport "0A000" "TOAST storage changed during query planning")))
        (unless (eq (getf spec :columns) (pg-class-columns table))
          (ereport "0A000" "table definition changed during query planning"))
        (when (and index (not (select-index-current-p table index)))
          ;; The old index reader remains registered until transaction end.
          (setf (getf spec :path) (make-select-scan-path :kind :seq-scan)))))))

(defmacro with-registered-select-relations ((registered-relations relations)
                                            &body body)
  "Protect every relation and access path before evaluating BODY.

All heap/index reader registrations happen before the catalog mutex is taken
to revalidate the planned table and index identities.  This keeps the
registration-to-DDL handoff free of a lock-order inversion."
  (let ((relation-list (gensym "RELATIONS-"))
        (xid (gensym "XID-"))
        (registered-relfilenodes (gensym "REGISTERED-RELFILENODES-")))
    `(let ((,relation-list (mapcar #'copy-list ,relations)))
       (with-registered-relation-access
           (,registered-relfilenodes ,xid :read)
           (select-relation-reader-relfilenodes ,relation-list)
           (revalidate-select-relations ,relation-list)
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

(defstruct select-index-scan
  "A scoped cursor and lease, or a sealed spool of its unread TIDs."
  cursor lease spool)

(defvar *select-index-scans* nil)

(defstruct select-index-lease-domain
  "The fixed, sorted index nodes available to a query and its nested plans."
  nodes)

(defstruct select-index-lease-group
  "Scoped leases for a domain, paired with their nodes in descending order."
  domain leases)

(defvar *select-index-lease-domain* nil)

(defvar *select-index-lease-groups* nil)

(defun select-index-lease-owner-count (lease)
  "Count supported scan and group owners of LEASE."
  (+ (count lease *select-index-scans* :key #'select-index-scan-lease :test #'eq)
     (loop for group in *select-index-lease-groups*
           sum (count lease (select-index-lease-group-leases group) :key #'cdr :test #'eq))))

(defun check-select-index-lease-groups ()
  "Check group ownership and that held nodes form an ascending domain prefix."
  (dolist (group *select-index-lease-groups*)
    (let* ((domain (select-index-lease-group-domain group))
           (pairs (select-index-lease-group-leases group))
           (nodes (select-index-lease-domain-nodes domain))
           (held (reverse (loop for pair in pairs
                                when (btree-read-lease-active-p (cdr pair))
                                  collect (car pair)))))
      (unless (and (eq domain *select-index-lease-domain*)
                   (equal (mapcar #'car pairs) (reverse nodes))
                   (equal held (subseq nodes 0 (length held))))
        (error "index lease group is outside its ordered domain prefix"))
      (dolist (pair pairs)
        (let ((lease (cdr pair)))
          (unless (and (btree-read-lease-p lease)
                       (member lease *btree-read-leases* :test #'eq)
                       (eq (btree-read-lease-thread lease) sb-thread:*current-thread*)
                       (not (btree-read-lease-writer-reentrant-p lease))
                       (member (btree-read-lease-status lease) '(:held :released))
                       (= 1 (select-index-lease-owner-count lease))
                       (eq (btree-read-lease-lock-state lease)
                           (btree-relation-lock (car pair))))
            (error "index lease group ownership mismatch"))))))
  (values))

(defun select-index-domain-covered-p (domain)
  "Return true if one group holds every lease in DOMAIN."
  (some (lambda (group)
          (and (eq domain (select-index-lease-group-domain group))
               (= (length (select-index-lease-domain-nodes domain))
                  (length (select-index-lease-group-leases group)))
               (every (lambda (pair) (btree-read-lease-active-p (cdr pair)))
                      (select-index-lease-group-leases group))))
        *select-index-lease-groups*))

(defun call-with-select-index-lease-group (domain function)
  "Call FUNCTION with full domain coverage, acquiring a new group if needed.

Reject held ungrouped leases before reacquisition so lock order is preserved."
  (if (select-index-domain-covered-p domain)
      (funcall function)
      (progn
        (check-select-index-lease-groups)
        (when (select-index-lease-domain-nodes domain)
          (dolist (lease *btree-read-leases*)
            (when (and (btree-read-lease-active-p lease)
                       (/= 1 (loop for group in *select-index-lease-groups*
                                   sum (count lease (select-index-lease-group-leases group)
                                              :key #'cdr :test #'eq))))
              (error "cannot reacquire an index domain under an ungrouped read lease"))))
        (call-with-btree-relation-read-leases
         (select-index-lease-domain-nodes domain)
         (lambda (leases)
           (let ((*select-index-lease-groups*
                   (cons (make-select-index-lease-group :domain domain :leases leases)
                         *select-index-lease-groups*)))
             (funcall function)))))))

(defun call-with-select-index-lease-domain (relfilenodes function)
  "Call FUNCTION within a fixed, ordered index domain.

Nested plans may reuse a subset but cannot expand the domain.  Reject held
leases before initializing a nonempty new domain."
  (let ((nodes (btree-ordered-relfilenodes relfilenodes)))
    (if *select-index-lease-domain*
        (progn
          (unless (subsetp nodes (select-index-lease-domain-nodes *select-index-lease-domain*)
                           :test #'eql)
            (error "nested query expands its enclosing index lease domain"))
          (call-with-select-index-lease-group *select-index-lease-domain* function))
        (let ((*select-index-lease-domain* (make-select-index-lease-domain :nodes nodes)))
          (when (and nodes (some #'btree-read-lease-active-p *btree-read-leases*))
            (error "cannot initialize an index domain under an existing read lease"))
          (dolist (node nodes) (btree-ensure-init-before-read node))
          (call-with-select-index-lease-group *select-index-lease-domain* function)))))

(defun call-with-current-select-index-domain (relfilenode function)
  "Check cursor membership and restore full domain coverage when needed."
  (if *select-index-lease-domain*
      (progn
        (unless (member relfilenode (select-index-lease-domain-nodes *select-index-lease-domain*)
                        :test #'eql)
          (error "cursor index is outside the enclosing plan lease domain"))
        (call-with-select-index-lease-group *select-index-lease-domain* function))
      (funcall function)))

(defun release-select-index-lease-groups ()
  "Release held group leases in descending node order.

Use a fresh list and finish the releases after a timeout before reporting it."
  (let ((pairs (sort (loop for group in *select-index-lease-groups*
                           append (loop for pair in (select-index-lease-group-leases group)
                                        when (btree-read-lease-active-p (cdr pair))
                                          collect pair))
                     #'> :key #'car))
        (pending nil))
    (sb-sys:without-interrupts
      (dolist (pair pairs)
        (handler-case (release-btree-read-lease (cdr pair))
          (sb-ext:timeout (condition) (unless pending (setf pending condition)))))
      (when pending (error pending))))
  (values))

(defun stream-select-index-cursor (scan callback)
  "Deliver TIDs from the cursor or its detached spool.

Without parking, :STOP leaves the cursor lazy.  After detachment, it stops
spool delivery without invoking the callback for the remaining tail."
  (loop
    (when (select-index-scan-spool scan)
      (return
        (map-query-row-spool
         (select-index-scan-spool scan)
         (lambda (row index)
           (declare (ignore index))
           (funcall callback (cons (aref row 0) (aref row 1)))))))
    (let ((tid (btree-cursor-next-tid (select-index-scan-cursor scan))))
      (unless tid (return (values)))
      (when (eq (funcall callback tid) :stop) (return :stop)))))

(defun call-with-select-index-cursor (cursor lease callback)
  "Stream CURSOR under its lease and own any detached spool until scope exit."
  (unless (and (btree-read-lease-active-p lease)
               (eq (btree-read-lease-thread lease) sb-thread:*current-thread*)
               (eq (btree-read-lease-lock-state lease)
                   (btree-relation-lock (btree-equal-cursor-relfilenode cursor))))
    (error "index cursor requires its owning read lease"))
  (with-query-temp-files
    (let ((scan (make-select-index-scan :cursor cursor :lease lease)))
      (unwind-protect
           (let ((*select-index-scans* (cons scan *select-index-scans*)))
             (stream-select-index-cursor scan callback))
        (sb-sys:without-interrupts
          (when (select-index-scan-spool scan)
            (close-query-row-spool (select-index-scan-spool scan))))))))

(defun check-select-index-scan-ownership ()
  "Check that every held lease has one supported, non-writer-reentrant owner."
  (check-select-index-lease-groups)
  (dolist (lease *btree-read-leases*)
    (when (btree-read-lease-active-p lease)
      (unless (and (not (btree-read-lease-writer-reentrant-p lease))
                   (= 1 (select-index-lease-owner-count lease)))
        (error "cannot park while owning an unsupported B-tree read lease"))))
  (dolist (scan *select-index-scans*)
    (let ((lease (select-index-scan-lease scan))
          (spool (select-index-scan-spool scan)))
      (unless (and (member lease *btree-read-leases* :test #'eq)
                   (= 1 (select-index-lease-owner-count lease))
                   (eq (btree-read-lease-thread lease) sb-thread:*current-thread*)
                   (if spool
                       (and (null (select-index-scan-cursor scan))
                            (not (btree-read-lease-active-p lease))
                            (query-row-spool-sealed-p spool))
                       (and (select-index-scan-cursor scan)
                            (btree-read-lease-active-p lease))))
        (error "index scan ownership mismatch"))))
  (values))

(defun detach-select-index-scan (scan)
  "Save unread TIDs before releasing SCAN's lease.

Read a cursor copy without fetching or evaluating tuples.  Publish the sealed
spool and clear the cursor before release, which can report a deadline after
discharging the lease.  On failure before publication, close the partial spool
and retain the original cursor and lease.  An existing spool keeps its position."
  (unless (select-index-scan-spool scan)
    (let ((cursor (clone-btree-equal-cursor (select-index-scan-cursor scan)))
          (spool (make-query-row-spool 1))
          (installed nil))
      (unwind-protect
           (progn
             (loop for tid = (btree-cursor-next-tid cursor)
                   while tid
                   do (check-for-interrupts)
                      (query-row-spool-append spool (vector (car tid) (cdr tid))))
             (query-row-spool-seal spool)
             (sb-sys:without-interrupts
               (setf (select-index-scan-spool scan) spool
                     (select-index-scan-cursor scan) nil
                     installed t)
               (release-btree-read-lease (select-index-scan-lease scan))))
        (sb-sys:without-interrupts
          (unless installed (close-query-row-spool spool))))))
  (values))

(defun detach-select-index-scans ()
  "Detach owned scans and release group leases before parking.

If a later scan fails, earlier completed detachments remain published for their
scopes to clean up."
  (check-select-index-scan-ownership)
  (dolist (scan *select-index-scans*) (detach-select-index-scan scan))
  (release-select-index-lease-groups)
  (when (some #'btree-read-lease-active-p *btree-read-leases*)
    (error "index detachment left a held B-tree read lease"))
  (values))

(defun select-toast-index-reader (relfilenode function)
  "Run a TOAST lookup under the SELECT domain, or a standalone index lease."
  (if *select-index-lease-domain*
      (call-with-current-select-index-domain relfilenode function)
      (call-with-btree-relation-read-lock relfilenode function)))

(defun statement-protection-snapshot (&optional (fallback *current-snapshot*))
  "Return an enclosing active statement snapshot that protects tuple lifetimes."
  (or (loop for registration in *statement-snapshot-registrations*
            when (snapshot-registration-active-p registration)
              return (snapshot-registration-snapshot registration))
      fallback))

(defvar *select-toast-demand* nil
  "NIL means full materialization; otherwise a cons of table and needed slots.")

(defvar *select-toast-slice-plan* nil
  "A recognized expression, owning table, and bytea slot for this scan.")

(defun materialize-select-tuple (table tuple snapshot &key (index-reader #'select-toast-index-reader) demand)
  "Replace external attributes with SQL values, omitting proven unused slots.
The parent is visible and SNAPSHOT remains active throughout the fetch. TOAST
chunk visibility follows the parent and is not independently filtered."
  (let ((values (htup-values tuple))
        (columns (pg-class-columns table)))
    (loop for i below (length values)
          for value = (aref values i)
          when (heap-external-varlena-p value)
            do (if (and demand
                        (eq table (car demand))
                        (not (member (1+ i) (cdr demand))))
                   (setf (aref values i) +null+)
                   (let* ((toast (pg-class-toast table))
                          (type (pg-column-type (aref columns i))))
                     (unless toast
                       (ereport "XX001" "external value has no owning TOAST relation"))
                     (let ((bytes (toast-fetch-value toast value snapshot :index-reader index-reader)))
                       (setf (aref values i)
                             (case type
                               ((:varchar :text :json) (bytes->string bytes))
                               (:bytea bytes)
                               (:jsonb (sql-jsonb-from-octets bytes))
                               (:numeric (sql-numeric-from-varlena-payload bytes))
                               (otherwise (ereport "XX001" "external value has unsupported type"))))))))
    tuple))

(defun unchanged-tuple-toast-pointers (tuple assignments)
  "Return pointers for columns not assigned by this update.
TUPLE is the physical version actually claimed, including after an EPQ recheck."
  (let* ((values (htup-values tuple))
         (reuse (make-array (length values) :initial-element nil)))
    (loop for value across values for i from 0
          when (and (heap-external-varlena-p value)
                    (notany (lambda (assignment)
                              (= (pg-column-attnum (car assignment)) (1+ i)))
                            assignments))
            do (setf (aref reuse i) value))
    reuse))

(defun delete-tuple-toast-values (table tuple xid cid &optional reuse)
  "Mark the physical external attributes of a claimed parent tuple deleted.
TUPLE must still contain its physical pointers: materialization replaces them
with SQL values. Detach scan leases before taking the TOAST index lock, and take
that lock before the write lock. The transaction must abort on partial failure."
  (let ((pointers (loop for pointer across (htup-values tuple) for i from 0
                        when (and (heap-external-varlena-p pointer)
                                  (not (and reuse (aref reuse i))))
                          collect pointer)))
    (when (plusp (length pointers))
      (let ((toast (pg-class-toast table)))
        (unless toast
          (ereport "XX001" "external value has no owning TOAST relation"))
        (detach-select-index-scans)
        (with-btree-relation-locks ((list (pg-toast-index-relfilenode toast)))
          (with-write-lock
            (ensure-current-table table)
            (loop for pointer in pointers
                  do (toast-delete-value-unlocked toast pointer xid cid))))))))

(defun scan-select-index (table path snapshot callback &optional environment)
  "Scan matching index TIDs and pass visible heap tuples to CALLBACK.

Evaluate the parameter key once before restoring domain coverage; key evaluation
can itself park.  A new cursor restores coverage if an earlier park released it."
  (let* ((idx (select-scan-path-index path))
         (relfilenode (pg-class-relfilenode table))
         (key (select-scan-index-key table path environment)))
    (when key
      (call-with-current-select-index-domain
       (pg-index-relfilenode idx)
       (lambda ()
         (call-with-btree-equal-cursor
          (pg-index-relfilenode idx) key
          (lambda (cursor lease)
            (call-with-select-index-cursor
             cursor lease
             (lambda (tid)
               (check-for-interrupts)
               (let ((tuple (heapam-get-tuple relfilenode (car tid) (cdr tid)
                                              (pg-class-columns table))))
                 (when (and tuple (htup-visible-p tuple snapshot))
                   (let ((row (materialize-select-tuple table tuple snapshot :demand *select-toast-demand*)))
                     (let ((*select-toast-demand* nil))
                       (funcall callback row))))))))))))))

(defun scan-select-path (table relfilenode path snapshot callback
                         &optional environment)
  (ecase (select-scan-path-kind path)
    (:seq-scan
     (heapam-scan relfilenode (pg-class-columns table) snapshot
                  (lambda (tuple)
                    (let* ((*toast-slice-current*
                             (when (and *select-toast-slice-plan*
                                        (eq table (car *select-toast-slice-plan*)))
                               (loop for (expression . slot) in (cdr *select-toast-slice-plan*)
                                     for value = (aref (htup-values tuple) (1- slot))
                                     when (heap-external-varlena-p value)
                                       collect (cons expression
                                                     (list (pg-class-toast table) value snapshot
                                                           (htup-values tuple))))))
                           (row (materialize-select-tuple table tuple snapshot :demand *select-toast-demand*)))
                      (let ((*select-toast-demand* nil)
                            (*select-toast-slice-plan* nil))
                        (funcall callback row))))))
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
  (let* ((width (or (getf relation :environment-width)
                    (reduce #'+ relations :key (lambda (spec) (getf spec :width))
                                          :initial-value 0)))
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
  "Rescan a parameterized index or LATERAL input for each left row.
The right input belongs to one left-row parameter epoch and cannot be replayed
for another left row.  LEFT joins null-extend only after that input is exhausted."
  (let* ((join-type (getf join :type))
         (qual (getf join :qual))
         (right-width (getf join :right-width))
         (null-right (make-array right-width :initial-element +null+))
         (stopped nil))
    (unless (member join-type '(:inner :cross :left))
      (error "Parameterized join does not support ~a joins" join-type))
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

(defun select-write-bytea-text (out bytes format)
  "Write PostgreSQL bytea text without building a value-sized string."
  (check-type bytes (vector (unsigned-byte 8)))
  (if (string= format "escape")
      (loop for byte across bytes
            do (cond ((= byte 92) (write-char #\\ out) (write-char #\\ out))
                     ((or (< byte 32) (> byte 126))
                      (write-char #\\ out)
                      (write-char (digit-char (ldb (byte 2 6) byte)) out)
                      (write-char (digit-char (ldb (byte 3 3) byte)) out)
                      (write-char (digit-char (ldb (byte 3 0) byte)) out))
                     (t (write-char (code-char byte) out))))
      (let ((digits "0123456789abcdef"))
        (write-char #\\ out) (write-char #\x out)
        (loop for byte across bytes
              do (write-char (char digits (ash byte -4)) out)
                 (write-char (char digits (logand byte 15)) out)))))

(defun select-row-field-writer (value type)
  "Return a repeatable character writer, or NIL for SQL NULL."
  (cond ((eq value +null+) nil)
        ((eq type :bytea)
         (let ((format (or (session-extra-setting-value "bytea_output") "hex")))
           (lambda (out) (select-write-bytea-text out value format))))
        ((eq type :jsonb) (lambda (out) (sql-jsonb-write value out)))
        ((and (member type '(:text :varchar :json)) (stringp value))
         (lambda (out) (write-string value out)))
        (t (let ((text (value->text value type)))
             (unless (stringp text) (error "invalid DataRow field"))
             (lambda (out) (write-string text out))))))

(defun select-row-utf8-write (writer sink &optional (limit #x7fffffff))
  (multiple-value-bind (counter metrics) (logical-encoding-sink limit)
    (let ((out (make-instance 'logical-utf8-output-stream
                              :buffer (lambda (octets)
                                        (funcall counter octets)
                                        (when sink (funcall sink octets))))))
      (funcall writer out)
      (logical-flush-utf8-output out)
      (multiple-value-bind (count checksum) (funcall metrics)
        (unless (= count (logical-utf8-size out)) (error "invalid DataRow field count"))
        (values count checksum)))))

(defun send-select-data-row (stream values targets)
  "Preflight every field, then write one bounded text DataRow."
  (unless (and (= (length values) (length targets)) (<= (length targets) #xffff))
    (error "invalid DataRow width"))
  (let ((fields nil) (size (+ 6 (* 4 (length targets)))))
    (loop for value across values for target in targets
          for writer = (select-row-field-writer value (getf target :type))
          do (multiple-value-bind (length checksum)
                 (if writer (select-row-utf8-write writer nil) (values nil nil))
               (when (and length (> length #x7fffffff)) (error "DataRow field too large"))
               (when length (incf size length))
               (push (list writer length checksum) fields)))
    (when (> size #x7fffffff) (error "DataRow frame too large"))
    (let ((complete nil) (last-byte nil))
      (labels ((emit (octets)
                 (when (plusp (length octets))
                   (when last-byte (write-byte last-byte stream))
                   (when (> (length octets) 1)
                     (write-sequence octets stream :end (1- (length octets))))
                   (setf last-byte (aref octets (1- (length octets)))))))
        (sb-sys:without-interrupts
          (unwind-protect
               (sb-sys:with-local-interrupts
                 (emit (vector (char-code #\D)))
                 (emit (u32-to-bytes size))
                 (emit (u16-to-bytes (length targets)))
                 (dolist (field (nreverse fields))
                   (destructuring-bind (writer length checksum) field
                     (emit (u32-to-bytes (if writer length #xffffffff)))
                     (when writer
                       (multiple-value-bind (actual actual-checksum)
                           (select-row-utf8-write writer #'emit length)
                         (unless (and (= length actual) (= checksum actual-checksum))
                           (error "DataRow field changed during output"))))))
                 (unless last-byte (error "empty DataRow frame"))
                 (write-byte last-byte stream)
                 (force-output stream)
                 (setf complete t))
            (unless complete (ignore-errors (close stream :abort t))))))))
  (values))

(defun send-select-row (stream row targets)
  (send-select-data-row stream (car row) targets))

(defun select-path-description (table path)
  (case (select-scan-path-kind path)
    (:index-scan
     (format nil "Index Scan using ~a on ~a"
             (pg-index-name (select-scan-path-index path))
             (pg-class-name table)))
    (:seq-scan
     (format nil "Seq Scan on ~a" (pg-class-name table)))
    (:result "Result")))
