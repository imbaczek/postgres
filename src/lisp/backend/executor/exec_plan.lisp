;;;; exec_plan.lisp -- S-expression query plans and executor support.

(in-package #:pg)

(defstruct hash-join-stats
  "Cumulative work across node loops and the largest charged build block."
  (candidates 0)
  (spill-batches 0)
  (build-blocks 0)
  (peak-build-bytes 0)
  key-probes)

(defstruct plan-node-metadata
  estimated-rows
  estimated-cost
  estimated-startup-cost
  (actual-row-count 0)
  (filtered-row-count 0)
  resources
  sort-methods
  hash-join-stats
  (actual-loops 0)
  (actual-startup-time-ticks 0)
  (actual-total-time-ticks 0)
  selection-rule)

(defun plan-node-metadata-actual-rows (metadata)
  "Compute the row average from all loops, including loops with no output."
  (let ((loops (plan-node-metadata-actual-loops metadata)))
    (when (plusp loops)
      (/ (plan-node-metadata-actual-row-count metadata) loops))))

(defstruct (query-plan (:constructor %make-query-plan))
  logical
  physical
  alternatives
  prepared
  (metadata (make-hash-table :test #'eq)))

(defvar *query-plan-instrumentation-p* nil
  "NIL disables node statistics, :ROWS collects counts, and T also collects timing.")

(defvar *explain-estimates-p* nil
  "Capture planner estimates for EXPLAIN with costs enabled.")

(defun internal-time-ticks->milliseconds (ticks)
  (* 1000.0d0 (/ ticks internal-time-units-per-second)))

(defun query-plan-sexp (plan)
  "Return the inspectable logical/physical plan artifact."
  (list :logical (query-plan-logical plan)
        :physical (query-plan-physical plan)
        :alternatives (query-plan-alternatives plan)))

(defun reorderable-inner-query-p (relations joins)
  (and (> (length relations) 1)
       (every (lambda (relation) (not (getf relation :virtual-p))) relations)
       (every (lambda (join)
                (member (getf join :type) '(:inner :cross)))
              joins)))

(defun relation-path-candidates (prepared relation-index joined-indices)
  (let* ((relations (getf prepared :relations))
         (relation (nth relation-index relations))
         (table (getf relation :table))
         (offset (getf relation :offset))
         (where-paths
           (loop for qual in (where-conjuncts (getf prepared :where))
                 append (index-paths-for-equality table qual offset)))
         (join-paths
           (when joined-indices
             (index-paths-for-relation-joins
              table relation-index relations joined-indices
              (getf prepared :joins)))))
    (cons (make-select-scan-path :kind :seq-scan)
          (append where-paths join-paths))))

(defun choose-greedy-select-paths (prepared)
  "Greedily add the cheapest INNER/CROSS relation, preserving source Var slots.

The bounded search follows joinrels.c's level-by-level idea without building
PostgreSQL's full join-relation graph. Each parameterized path can reference
only joined Vars. Join quals normally filter above the reordered tree. A
physical two-way nested loop can apply a supported integer equality at the join."
  (let* ((relations (getf prepared :relations))
         (paths (make-array (length relations)))
         (remaining (loop for index below (length relations) collect index))
         (order '())
         (joined '())
         (outer-rows 1.0d0))
    (with-write-lock
      (loop while remaining
            do (let ((best-index nil)
                     (best-path nil)
                     (best-cost nil))
                 (dolist (index remaining)
                   (let* ((table (getf (nth index relations) :table))
                          (candidates
                            (relation-path-candidates prepared index joined))
                          (path (choose-cheapest-select-path
                                 table candidates outer-rows))
                          (cost (nested-loop-inner-path-cost
                                 table path outer-rows)))
                     (when (or (null best-cost) (< cost best-cost))
                       (setf best-index index
                             best-path path
                             best-cost cost))))
                 (setf (aref paths best-index) best-path
                       remaining (remove best-index remaining)
                       order (append order (list best-index))
                       joined (append joined (list best-index)))
                 (let ((table (getf (nth best-index relations) :table)))
                   (setf outer-rows
                         (min 1.0d12
                              (max 1.0d0
                                   (* outer-rows
                                      (max 1.0d-6
                                           (select-scan-path-row-estimate
                                            table best-path))))))))))
    (setf (getf prepared :join-order) order)
    (values (coerce paths 'list) order)))

(defun choose-source-order-select-paths (prepared)
  (let* ((relations (getf prepared :relations))
         (joins (getf prepared :joins))
         (paths '())
         (outer-rows 1.0d0))
    (with-write-lock
      (loop for relation in relations
            for index from 0
            for table = (getf relation :table)
            for offset = (getf relation :offset)
            for path =
                     (cond
                       ((getf relation :virtual-p)
                        (make-select-scan-path :kind :seq-scan))
                       ((zerop index)
                        (choose-select-scan-path table (getf prepared :where) offset))
                       (t
                        (choose-join-inner-path table (nth (1- index) joins)
                                                outer-rows offset)))
            do (push path paths)
               (setf outer-rows
                     (min 1.0d12
                          (max 1.0d0
                               (* outer-rows
                                  (max 1.0d-6
                                       (if (getf relation :virtual-p)
                                           100.0d0
                                           (select-scan-path-row-estimate
                                            table path)))))))))
    (nreverse paths)))

(defun select-plan-path-candidates (prepared)
  "Cost scan paths and retain a SeqScan-only fallback alternative."
  (when *cte-pull-planning-p*
    (return-from select-plan-path-candidates
      (values (loop repeat (length (getf prepared :relations))
                    collect (make-select-scan-path :kind :seq-scan)) nil)))
  (let* ((relations (getf prepared :relations))
         (table (getf prepared :table))
         (selected
           (cond
             ((null relations) nil)
             ((reorderable-inner-query-p relations (getf prepared :joins))
              (first (multiple-value-list
                      (choose-greedy-select-paths prepared))))
             ((and (null (getf prepared :joins))
                   (getf (first relations) :virtual-p))
              (list (make-select-scan-path :kind :seq-scan)))
             ((null (getf prepared :joins))
              (list (with-write-lock
                      (choose-ordered-limit-scan-path
                       table prepared (choose-select-scan-path table (getf prepared :where))))))
             (t (choose-source-order-select-paths prepared))))
         (seq-paths (loop repeat (length relations)
                          collect (make-select-scan-path :kind :seq-scan)))
         (alternatives
           (if (some (lambda (path)
                       (eq (select-scan-path-kind path) :index-scan)) selected)
               (list selected seq-paths)
               (list seq-paths))))
    (values selected alternatives)))

(defun select-scan-plan-form (relation-index relation path physical-p)
  (let ((table (getf relation :table)))
    (cond
      ((getf relation :cte-worktable) (list :worktable-scan relation-index 0))
      ((getf relation :cte) (list :cte-scan relation-index 0))
      ((getf relation :subquery-plan)
       (list :subquery-scan relation-index 0
             (getf relation (if physical-p :subquery-physical :subquery-logical))))
      ((not physical-p) (list :scan relation-index (pg-class-oid table)))
      ((eq (select-scan-path-kind path) :index-scan)
       (let ((range (select-scan-path-range path)))
         (list* :index-scan relation-index (pg-class-oid table)
                (pg-index-oid (select-scan-path-index path))
                (select-scan-path-qual path) (select-scan-path-key path)
                (select-scan-path-parameter-expr path)
                (when range
                  (list (if (select-index-range-backward-p range) :backward :forward))))))
      (t (list :seq-scan relation-index (pg-class-oid table))))))

(defun hash-join-key-slots (join)
  "Extract bare equijoin keys and indicate whether they cover the full qualifier."
  (let ((width (getf join :left-width))
        (right-width (getf join :right-width))
        (terms (where-conjuncts (getf join :qual)))
        (left nil) (right nil) (types nil))
    (dolist (qual terms)
      (multiple-value-bind (a b equality-p) (equality-column-references qual)
        (when (and equality-p (eq (expr-type a) (expr-type b))
                   (hash-join-key-type-p (expr-type a)))
          (when (> (second a) (second b)) (rotatef a b))
          (when (and (<= 1 (second a) width)
                     (< width (second b) (1+ (+ width right-width))))
            (push (1- (second a)) left)
            (push (- (second b) width 1) right)
            (push (expr-type a) types)))))
    (when types
      (let ((covered-p (= (length types) (length terms))))
        (values (list (coerce (nreverse left) 'vector)
                      (coerce (nreverse right) 'vector)
                      (coerce (nreverse types) 'vector))
                covered-p)))))

(defconstant +hash-join-entry-overhead-bytes+ 112)

(defun estimate-hash-build-bytes (table rows)
  "Estimate retained SQL row bytes without fetching or detoasting tuples."
  (let* ((statistics (pg-class-statistics table))
         (width (or (and statistics (pg-table-statistics-average-tuple-bytes statistics))
                    (ceiling (* +blcksz+ (table-page-estimate table)) (max 1 rows))))
         (toast (pg-class-toast table)))
    ;; Charge the hash entry, record and row-vector overhead, and one relation's
    ;; origin and presence. Heap and TOAST bytes approximate decoded values,
    ;; which can be larger for compressed or boxed data.
    (+ (* rows (+ width +hash-join-entry-overhead-bytes+ 64 64 16))
       (if toast (* +blcksz+ (buf-nblocks (pg-toast-relfilenode toast))) 0))))

(defun hash-join-streaming-limit (prepared)
  "Return an early row demand when no upper operator consumes all join rows."
  (let ((limit (getf prepared :limit)))
    (when (and (typep limit '(integer 0))
               (not (or (getf prepared :order) (getf prepared :where)
                        (getf prepared :aggregate-p) (getf prepared :distinct)
                        (getf prepared :has-having) (getf prepared :windows)
                        (getf prepared :set-functions))))
      limit)))

(defun hash-join-estimated-spill-pages (build-bytes left-bytes right-bytes preserve-probe-p)
  "Estimate sequential spill work when the retained build exceeds work_mem."
  (when (> build-bytes
           (hash-join-initial-build-memory (effective-query-work-memory-bytes) preserve-probe-p))
    (ceiling (+ left-bytes right-bytes) +blcksz+)))

(defun prefer-streaming-nested-loop-p (prepared join left right relations spill-pages left-rows right-rows)
  "Compare a lazy nested loop's first requested rows with spill startup.
Use a simple equality with statistics so the estimate has a match rate."
  (let* ((limit (and spill-pages (eq (getf join :type) :inner)
                     (hash-join-streaming-limit prepared)))
         (selectivity (and limit (explain-equality-join-selectivity
                                  join left right relations left-rows right-rows t))))
    (when (and selectivity (plusp selectivity))
      (let ((candidates (/ limit selectivity)))
        ;; Charge both full scans to both paths. The lazy loop can stop
        ;; inside its first materialization, before hashing or spill writes.
        (and (< candidates (* left-rows right-rows))
             (< (* 0.0225d0 candidates) (+ spill-pages (* 0.02d0 (+ left-rows right-rows)))))))))

(defun select-two-table-hash-input (relations joins paths prepared)
  "Choose a two-way hash join when full scans make repeated probing costly."
  (let ((join (first joins)))
    (when (and (= (length relations) (length paths) 2) (null (rest joins))
               (member (getf join :type) '(:inner :left :right :full))
               (not (getf join :lateral-dependent))
               (every (lambda (relation)
                        (and (getf relation :table) (not (getf relation :virtual-p))
                             (not (getf relation :row-source))
                             (not (getf relation :lateral-dependent))))
                      relations)
               (every (lambda (path) (eq (select-scan-path-kind path) :seq-scan)) paths)
               (not (analyzed-expression-volatile-p (getf join :qual))))
      (multiple-value-bind (keys covered-p) (hash-join-key-slots join)
        (when keys
          (with-write-lock
            (let ((left (getf (first relations) :table))
                  (right (getf (second relations) :table)))
              (ensure-current-table left)
              (ensure-current-table right)
              (let ((left-rows (table-row-estimate left))
                    (right-rows (table-row-estimate right)))
                (when (> (* left-rows right-rows) (* 64 (+ left-rows right-rows)))
                  (let* ((left-bytes (estimate-hash-build-bytes left left-rows))
                         (right-bytes (estimate-hash-build-bytes right right-rows))
                         (build-left-p (< left-bytes right-bytes))
                         (preserve-probe-p (member (getf join :type)
                                                   (if build-left-p '(:right :full) '(:left :full))))
                         (spill-pages (hash-join-estimated-spill-pages (min left-bytes right-bytes)
                                                                       left-bytes right-bytes preserve-probe-p))
                         (left-node (select-scan-plan-form 0 (first relations) (first paths) t))
                         (right-node (select-scan-plan-form 1 (second relations) (second paths) t)))
                    (unless (prefer-streaming-nested-loop-p prepared join left-node right-node relations
                                                            spill-pages left-rows right-rows)
                      (list :hash-join (list* :hash-keys keys :hash-qual-covered-p covered-p
                                              :build-left-p build-left-p
                                              :hash-spill-pages spill-pages join)
                            left-node right-node))))))))))))

(defun select-plan-input-form (prepared physical-p paths)
  (let* ((relations (getf prepared :relations))
         (joins (getf prepared :joins))
         (join-order (getf prepared :join-order))
         (hash-input (when physical-p (select-two-table-hash-input relations joins paths prepared))))
    (when hash-input (return-from select-plan-input-form hash-input))
    (if (null relations)
        '(:result)
        (if join-order
            (let ((input nil)
                  (left-width 0))
              (dolist (relation-index join-order)
                (let* ((relation (nth relation-index relations))
                       (path (or (nth relation-index paths)
                                 (make-select-scan-path :kind :seq-scan)))
                       (right (select-scan-plan-form
                               relation-index relation path physical-p)))
                  (if input
                      (let ((join (list :type :inner :qual '(:bool t)
                                        :left-width left-width
                                        :right-width (getf relation :width)
                                        :right-offset (getf relation :offset)
                                        :reordered t)))
                        (setf input
                              (list (if physical-p :nested-loop-join :join)
                                    join input right)))
                      (setf input right))
                  (incf left-width (getf relation :width))))
              input)
            (let ((input
                    (select-scan-plan-form
                     0 (first relations) (or (first paths)
                                             (make-select-scan-path
                                              :kind :seq-scan)) physical-p)))
              (loop for join in joins
                    for relation-index from 1
                    for relation = (nth relation-index relations)
                    for path = (or (nth relation-index paths)
                                   (make-select-scan-path :kind :seq-scan))
                    for right = (select-scan-plan-form
                                 relation-index relation path physical-p)
                    do (setf input
                             (list (if physical-p :nested-loop-join :join)
                                   join input right)))
              input)))))

(defun select-reordered-join-qual (prepared)
  (let ((quals (remove '(:bool t) (mapcar (lambda (join) (getf join :qual))
                                          (getf prepared :joins))
                       :test #'equal)))
    (when quals
      (reduce (lambda (left right)
                (list :and left right :type :bool :typmod -1))
              (rest quals) :initial-value (first quals)))))

(defun prepared-sorted-aggregate-p (prepared)
  "Choose sorted aggregation for non-count transitions or record group keys.
Anonymous records lack hash support in C. Adjacent SQL equality also permits a
record comparison to stop before an unsupported later field."
  (or (some (lambda (descriptor) (not (eq (getf descriptor :kind) :count)))
            (getf prepared :aggregates))
      (some (lambda (key) (eq (expr-type key) :record))
            (getf prepared :group-keys))))

(defun select-distinct-plan-kind (targets physical-p)
  "Choose the DISTINCT implementation for the projected target types.
Anonymous records have ordering support but no hash support in C typcache, so a
physical plan containing one must sort and compare adjacent rows."
  (cond ((not physical-p) :distinct)
        ((some (lambda (target) (eq (expr-type (getf target :expr)) :record)) targets)
         :sorted-distinct)
        (t :hash-distinct)))

(defun constant-distinct-record-p (expression prepared)
  "Prove that a DISTINCT record key is constant through supported projections.
This is a bounded proof, not a general constant-expression test. Follow column
aliases only through pullable subqueries and unshared CTEs, and accept a whole
row only when its source has no relations and every target is a literal,
parameter, or cast thereof. Stop at query fences, volatile targets, lateral or
worktable sources, and potentially null-extending joins. For aggregate output,
first translate the output slot to its group key. Failure to prove constancy
returns NIL and leaves the key in DISTINCT.

The window constant predicate is insufficient here: it does not follow source
lineage or recognize parameters. The pull-up fences follow the relevant cases
of C's is_simple_subquery, rather than claiming a complete equivalent."
  (labels ((constant-p (form)
             (case (first form)
               ((:const :bool :parameter) t)
               (:cast (constant-p (second form)))
               (otherwise nil)))
           (nonnullable-p (query)
             (every (lambda (join) (member (getf join :type) '(:inner :cross)))
                    (getf query :joins)))
           (pullable-p (query)
             ;; Match the relevant fences in C is_simple_subquery.
             (and query
                  (notany (lambda (target) (cte-expression-volatile-p (getf target :expr)))
                          (getf query :targets))
                  (notany (lambda (key) (getf query key))
                          '(:set-operation :aggregate-p :group-keys :has-having
                            :set-functions :windows :order :limit :distinct :ctes))))
           (source-query (source)
             (unless (or (getf source :lateral-dependent)
                         (getf source :cte-worktable))
               (or (getf source :subquery-prepared)
                   (let ((cte (getf source :cte)))
                     (when (and cte (not (sql-cte-shared-p cte)))
                       (sql-cte-prepared cte))))))
           (record-constant-p (form query)
             (when (nonnullable-p query)
               (let ((sources (or (getf query :source-relations)
                                  (getf query :relations))))
                 (case (first form)
                   (:whole-row
                    (let* ((source (nth (getf (cdddr form) :source-index) sources))
                           (child (source-query source)))
                      (and (pullable-p child) (not (getf child :relations))
                           (every (lambda (target) (constant-p (getf target :expr)))
                                  (getf child :targets)))))
                   (:col
                    (let* ((slot (1- (second form)))
                           (source (find-if
                                    (lambda (source)
                                      (<= (getf source :offset) slot
                                          (1- (+ (getf source :offset)
                                                 (getf source :width)))))
                                    sources))
                           (child (and source (source-query source))))
                      (and (pullable-p child)
                           (record-constant-p
                            (getf (nth (- slot (getf source :offset))
                                       (getf child :targets)) :expr)
                            child)))))))))
    (and (eq (expr-type expression) :record)
         (let ((source-expression
                 (if (getf prepared :aggregate-p)
                     ;; Translate the grouped output slot once to its source key.
                     (when (eq (first expression) :col)
                       (nth (1- (second expression)) (getf prepared :group-keys)))
                     expression)))
           (and source-expression (record-constant-p source-expression prepared))))))

(defun select-distinct-keys (prepared)
  "Build the keys for DISTINCT, placing ORDER BY keys first for sorted plans.
Remove duplicate target positions and prune proven constant record keys before
sorting and adjacent equality. The remaining key order controls sort priority."
  (let* ((targets (getf prepared :targets))
         (sorted-p (eq (select-distinct-plan-kind targets t) :sorted-distinct))
         (keys (remove-duplicates
                (append (when sorted-p (mapcar #'car (getf prepared :order)))
                        (loop for target in targets for position from 1
                              collect (projected-target-ref target position)))
                :key #'second :test #'eql :from-end t)))
    (if sorted-p
        (remove-if (lambda (key)
                     (constant-distinct-record-p
                      (getf (nth (1- (second key)) targets) :expr) prepared))
                   keys)
        keys)))

(defun equality-join-key-offsets (join qual compatible-types-p)
  "Return the two input offsets for a bare column equality with compatible types."
  (declare (dynamic-extent compatible-types-p))
  (multiple-value-bind (a b equality-p) (equality-column-references qual)
    (when (and equality-p (funcall compatible-types-p (expr-type a) (expr-type b)))
      (let* ((start (if (getf join :reordered)
                        (getf join :right-offset)
                        (getf join :left-width)))
             (end (+ start (getf join :right-width)))
             (width (+ (getf join :left-width) (getf join :right-width)))
             (a (1- (second a)))
             (b (1- (second b))))
        (when (and (<= 0 a) (< a width) (<= 0 b) (< b width))
          (cond
            ((and (not (<= start a (1- end))) (<= start b (1- end)))
             (cons a (- b start)))
            ((and (not (<= start b (1- end))) (<= start a (1- end)))
             (cons b (- a start)))))))))

(defun integer-join-key-offsets (join qual)
  "Recognize signed integer equality for executor paths that require integer keys."
  (equality-join-key-offsets join qual
                             (lambda (left right)
                               (and (member left '(:int2 :int4 :int8))
                                    (member right '(:int2 :int4 :int8))))))

(defun build-select-plan-tree (prepared physical-p paths)
  (let* ((input (select-plan-input-form prepared physical-p paths))
         (where (getf prepared :where))
         (aggregate-p (getf prepared :aggregate-p))
         (degenerate-having-p (and (getf prepared :has-having)
                                   (not (getf prepared :group-keys))
                                   (zerop (getf prepared :aggregate-count 0)))))
    (when (and (getf prepared :join-order) (not (eq (first input) :hash-join)))
      (let ((join-qual (select-reordered-join-qual prepared)))
        (when join-qual
          ;; A false or NULL constant rejects every joined row. Keep the filter
          ;; on a Result input so scans removed by this test are not initialized.
          (when (and (window-constant-expression-p join-qual)
                     (not (true-p (eval-expr join-qual #()))))
            (setf input '(:result)))
          (if (and physical-p
                   (= (length (getf prepared :join-order)) 2)
                   (eq (first input) :nested-loop-join)
                   (integer-join-key-offsets (second input) join-qual))
              ;; Both Vars have their final source slots at this two-way join,
              ;; even if scan order changed. Test the equality here so failed
              ;; pairs do not need an owned result row.
              (setf (getf (second input) :qual) join-qual)
              (setf input (list :filter join-qual input))))))
    (when degenerate-having-p
      ;; With no transitions or grouping keys, HAVING alone generates one row;
      ;; neither the FROM input nor its WHERE clause can change that group.
      (setf input '(:result) where nil))
    (when where
      (when (and (window-constant-expression-p where) (not (true-p (eval-expr where #()))))
        (setf input '(:result)))
      (setf input (list :filter where input)))
    (when aggregate-p
      (setf input
            (list (if physical-p
                      (if (prepared-sorted-aggregate-p prepared) :sorted-aggregate :hash-aggregate)
                      :aggregate)
                  (getf prepared :group-keys)
                  (if (prepared-sorted-aggregate-p prepared)
                      (getf prepared :aggregates) (getf prepared :aggregate-count))
                  input)))
    (when (getf prepared :having)
      (setf input (list :filter (getf prepared :having) input)))
    ;; Project shared keys after grouping; window stages append their slots before
    ;; set-returning expressions and final target projection.
    (when (getf prepared :window-keys)
      (setf input (list :window-input (getf prepared :window-keys) input)))
    (dolist (stage (window-plan-stages prepared))
      (setf input (list :window stage input)))
    ;; Record the pre-ProjectSet width, including window slots. The resumable
    ;; sort path uses it to distinguish source columns from generated values.
    (when (getf prepared :set-functions)
      (setf input (list :project-set (getf prepared :set-functions) input
                        (when (getf prepared :window-input-width)
                          (+ (getf prepared :window-input-width)
                             (length (getf prepared :windows)))))))
    (setf input (list :project (getf prepared :targets) input))
    (when (getf prepared :distinct)
      (setf input (list (select-distinct-plan-kind (getf prepared :targets) physical-p)
                        (select-distinct-keys prepared)
                        input)))
    (finish-select-plan-tree
     input prepared physical-p
     (and physical-p (null (rest paths))
          (let ((range (and (first paths) (select-scan-path-range (first paths)))))
            (and range (select-index-range-ordered-p range)))))))

(defun query-plan-node-shape (node)
  "Return NODE's child layout and whether slot two is a relation handle."
  (when (consp node)
    (case (first node)
      ((:scan :seq-scan :index-scan :cte-scan :worktable-scan)
       (values :leaf t))
      (:subquery-scan (values :fourth t))
      (:result :leaf)
      ((:with-ctes :window-input :window :filter :project :sort :limit
         :distinct :hash-distinct :sorted-distinct :project-set)
       :third)
      ((:top-n :aggregate :hash-aggregate :sorted-aggregate) :fourth)
      ((:join :nested-loop-join :hash-join :recursive-union) :pair)
      (:append :append))))

(defun query-plan-relation-node-p (node)
  "Whether NODE carries a relation handle in slot two."
  (nth-value 1 (query-plan-node-shape node)))

(defun map-query-plan-children (function node &optional (shape (query-plan-node-shape node)))
  "Visit immediate plan children in execution tree order.
CTE definitions and expression subplans belong to their own plan domains."
  (flet ((visit (child)
           (when child (funcall function child))))
    (case shape
      (:third (visit (third node)))
      (:fourth (visit (fourth node)))
      (:pair (visit (third node)) (visit (fourth node)))
      (:append (dolist (arm (second node)) (visit (second arm)))))))

(defun collect-query-plan-nodes (tree)
  "Collect the local plan nodes in preorder, leaving metadata opaque."
  (let ((nodes nil))
    (labels ((visit (node)
               (let ((shape (query-plan-node-shape node)))
                 (when shape
                   (push node nodes)
                   (map-query-plan-children #'visit node shape)))))
      (visit tree))
    (nreverse nodes)))

(defun rebase-select-plan-tree (tree offset)
  "Copy nested SELECT IR, rebasing relation handles but keeping local Var slots."
  (let ((copy (copy-tree tree)))
    (dolist (node (collect-query-plan-nodes copy))
      (when (query-plan-relation-node-p node)
        (incf (second node) offset)))
    copy))

(defun flatten-select-subplans (prepared)
  "Splice child trees into a single registration and index-lease domain."
  (let ((relations (copy-list (getf prepared :relations))))
    (dolist (relation (getf prepared :relations))
      (let ((child (getf relation :subquery-plan)))
        (when child
          (let ((offset (length relations)))
            (setf (getf relation :subquery-logical)
                  (rebase-select-plan-tree (query-plan-logical child) offset)
                  (getf relation :subquery-physical)
                  (rebase-select-plan-tree (query-plan-physical child) offset))
            (setf relations
                  (append relations (getf (query-plan-prepared child) :relations)))))))
    (append relations (expression-subquery-relations prepared))))

(defun plan-select-query (ast)
  "Prepare nested SELECTs together, then choose paths without holding the catalog mutex."
  (plan-prepared-select (prepare-select ast)))

(defun explain-sort-input-table (node relations)
  "Find the base scan for an input whose width can be approximated from its heap."
  (case (first node)
    ((:seq-scan :index-scan)
     (getf (nth (second node) relations) :table))
    (:project
     (when (every (lambda (target) (eq (first (getf target :expr)) :col)) (second node))
       (explain-sort-input-table (third node) relations)))
    (:window-input
     (when (every (lambda (key) (member (first key) '(:col :const))) (second node))
       (explain-sort-input-table (third node) relations)))
    ((:limit :filter) (explain-sort-input-table (third node) relations))))

(defconstant +query-sort-merge-fan-in+ 32)

(defun explain-heap-row-width (table)
  (/ (* 8192 (table-page-estimate table)) (max 1 (table-row-estimate table))))

(defun explain-sort-cost (input-cost rows output-rows table)
  "Estimate comparison CPU, temporary page transfers, and output CPU.
cost_tuplesort in optimizer/path/costsize.c supplies the comparison and
extraction model. Native work memory charges include record containers.
Heap bytes per row approximate the payload for a direct scan or
column-only projection. Estimate run counts from retained record bytes and
charge sequential I/O for the payload on each estimated merge pass."
  (let* ((tuples (max 2 rows))
         (memory (max 1 (effective-query-work-memory-bytes)))
         (heap-width (explain-heap-row-width table))
         (record-width (+ heap-width (query-spill-record-size #(0 #() #()))))
         (input-bytes (* rows record-width))
         (spills (>= (* output-rows record-width) memory))
         (bounded (and (< output-rows rows) (not spills)
                       (or (> tuples (* 2 output-rows)) (> input-bytes memory))))
         (comparison-width (if bounded (max 2 (* 2 output-rows)) tuples))
         (io-cost
           (if spills
               (let* ((runs (max 2 (ceiling input-bytes memory)))
                      (fan-in (min +query-sort-merge-fan-in+
                                   (max 2 (floor memory record-width))))
                      (passes (max 1 (ceiling (log runs fan-in)))))
                 (* 2 (ceiling (* rows heap-width) 8192) passes))
               0))
         (startup (+ input-cost (* 0.005d0 tuples (log comparison-width 2.0d0)) io-cost)))
    (values output-rows startup (+ startup (* 0.0025d0 output-rows)))))

(defun explain-window-cost (stage rows startup total table)
  "Cost ranking windows that emit rows without buffering peers or partitions.
Following cost_windowagg in costsize.c, charge each window function, key
comparison, and output tuple. A required internal sort is blocking."
  (let ((functions (getf stage :functions)))
    (unless (and rows total
                 (every (lambda (function)
                          (member (getf function :kind) '(:row-number :rank :dense-rank)))
                        functions))
      (return-from explain-window-cost rows))
    (when (getf stage :sort-required)
      (unless table (return-from explain-window-cost rows))
      (multiple-value-setq (rows startup total) (explain-sort-cost total rows rows table)))
    (let ((operators (+ (length functions) (length (getf stage :partition))
                        (length (getf stage :order)))))
      (values rows startup (+ total (* rows (+ 0.01d0 (* 0.0025d0 operators))))))))

(defun explain-equality-join-mcv-selectivity (left-stats right-stats left-mcv right-mcv)
  "Combine exact common-value matches and C's residual eqjoinsel_inner estimate."
  (let ((matches 0) (matched-product 0.0d0)
                    (left-unmatched 0.0d0)
                    (right-unmatched
                      (loop for (value . frequency) in right-mcv
                            unless (assoc value left-mcv :test #'analyze-statistics-value-equal-p) sum frequency)))
    (dolist (entry left-mcv)
      (let ((match (assoc (car entry) right-mcv :test #'analyze-statistics-value-equal-p)))
        (if match
            (progn (incf matches)
                   (incf matched-product (* (cdr entry) (cdr match))))
            (incf left-unmatched (cdr entry)))))
    (flet ((residual (stats common)
             (max 0.0d0 (- 1.0d0 (pg-column-statistics-null-fraction stats)
                           (reduce #'+ common :key #'cdr :initial-value 0.0d0))))
           (contribution (unmatched other-unmatched residual other-residual other-distinct other-common)
             (+ (if (> other-distinct other-common)
                    (/ (* unmatched other-residual) (- other-distinct other-common))
                    0.0d0)
                (if (> other-distinct matches)
                    (/ (* residual (+ other-residual other-unmatched)) (- other-distinct matches))
                    0.0d0))))
      (let ((left-residual (residual left-stats left-mcv))
            (right-residual (residual right-stats right-mcv)))
        (min 1.0d0
             (+ matched-product
                (min (contribution left-unmatched right-unmatched left-residual right-residual
                                   (pg-column-statistics-distinct-count right-stats) (length right-mcv))
                     (contribution right-unmatched left-unmatched right-residual left-residual
                                   (pg-column-statistics-distinct-count left-stats) (length left-mcv)))))))))

(defun planner-join-base-scan (node)
  "Recognize an input with unchanged base-table column numbering."
  (loop while (eq (first node) :filter) do (setf node (third node)))
  (when (member (first node) '(:seq-scan :index-scan)) node))

(defun planner-join-distinct-estimate (statistics rows table)
  "Use C's default distinct count, capped by a known input cardinality."
  (max 1 (if statistics
             (pg-column-statistics-distinct-count statistics)
             (min 200 (or rows (table-row-estimate table))))))

(defun explain-equality-join-selectivity (join left right relations &optional left-rows right-rows require-statistics-p)
  "Estimate typed equality without applying a full-table distribution to a filtered input."
  (let ((offsets (equality-join-key-offsets
                  join (getf join :qual)
                  (lambda (left right)
                    (or (and (eq left right) (hash-join-key-type-p left))
                        (and (member left '(:int2 :int4 :int8))
                             (member right '(:int2 :int4 :int8)))
                        (and (member left '(:text :varchar))
                             (member right '(:text :varchar))))))))
    (when offsets
      (let* ((left-scan (planner-join-base-scan left))
             (right-scan (planner-join-base-scan right))
             (left-relation (when left-scan (nth (second left-scan) relations)))
             (right-relation (when right-scan (nth (second right-scan) relations)))
             (left-table (getf left-relation :table))
             (right-table (getf right-relation :table)))
        (when (and left-table right-table)
          (let* ((left-attnum (- (1+ (car offsets))
                                 (if (getf join :reordered) (getf left-relation :offset 0) 0)))
                 (left-stats (when (eq (first left) :seq-scan)
                               (table-statistics-column left-table left-attnum)))
                 (right-stats (when (eq (first right) :seq-scan)
                                (table-statistics-column right-table (1+ (cdr offsets)))))
                 (left-mcv (when left-stats (pg-column-statistics-most-common-values left-stats)))
                 (right-mcv (when right-stats (pg-column-statistics-most-common-values right-stats))))
            (when (and require-statistics-p (not (and left-stats right-stats)))
              (return-from explain-equality-join-selectivity nil))
            (if (and left-mcv right-mcv)
                (explain-equality-join-mcv-selectivity left-stats right-stats left-mcv right-mcv)
                ;; eqjoinsel_inner uses the non-NULL fraction of each input and
                ;; the greater distinct count when paired MCV lists are absent.
                (/ (* (if left-stats (- 1.0d0 (pg-column-statistics-null-fraction left-stats)) 1.0d0)
                      (if right-stats (- 1.0d0 (pg-column-statistics-null-fraction right-stats)) 1.0d0))
                   (max (planner-join-distinct-estimate left-stats left-rows left-table)
                        (planner-join-distinct-estimate right-stats right-rows right-table))))))))))

(defun explain-hash-join-estimates (node outer inner relations)
  "Estimate input, hash and match work, including spill before the first output."
  (let* ((join (second node))
         (outer-rows (plan-node-metadata-estimated-rows outer))
         (inner-rows (plan-node-metadata-estimated-rows inner))
         (outer-cost (plan-node-metadata-estimated-cost outer))
         (inner-cost (plan-node-metadata-estimated-cost inner)))
    (when (and outer-rows inner-rows)
      (let* ((selectivity (or (explain-equality-join-selectivity
                               join (third node) (fourth node) relations outer-rows inner-rows) 0.01d0))
             (rows (max (case (getf join :type)
                          (:left outer-rows) (:right inner-rows) (:full (max outer-rows inner-rows)) (t 1))
                        (round (* outer-rows inner-rows selectivity))))
             (spill-pages (getf join :hash-spill-pages)))
        (if (and outer-cost inner-cost)
            (let* ((input-cost (+ outer-cost inner-cost (* 0.02d0 (+ outer-rows inner-rows))))
                   (build-left-p (getf join :build-left-p))
                   (probe (if build-left-p inner outer))
                   (build-cost (if build-left-p outer-cost inner-cost))
                   (build-rows (if build-left-p outer-rows inner-rows)))
              (values rows
                      (if spill-pages
                          (+ input-cost spill-pages)
                          (+ build-cost (* 0.02d0 build-rows)
                             (or (plan-node-metadata-estimated-startup-cost probe) 0.0d0)))
                      (+ input-cost (* 2 (or spill-pages 0)) (* 0.01d0 rows))))
            rows)))))

(defun explain-nested-loop-estimates (node outer inner relations)
  "Estimate independent Cartesian and simple equality nested loops."
  (let* ((join (second node))
         (qual (getf join :qual))
         (left (third node))
         (right (fourth node))
         (outer-rows (plan-node-metadata-estimated-rows outer))
         (inner-rows (plan-node-metadata-estimated-rows inner)))
    (when (and outer-rows inner-rows
               (member (getf join :type) '(:inner :cross))
               (not (physical-join-rescans-inner-p join right)))
      (let* ((cartesian-p (or (null qual) (and (eq (first qual) :bool) (eq (second qual) t))))
             (selectivity (if cartesian-p 1 (explain-equality-join-selectivity join left right relations outer-rows inner-rows))))
        (when selectivity
          (let* ((candidates (* outer-rows inner-rows))
                 (rows (if cartesian-p candidates (max 1 (round (* candidates selectivity)))))
                 (relation (when (member (first right) '(:seq-scan :index-scan))
                             (nth (second right) relations)))
                 (outer-total (plan-node-metadata-estimated-cost outer)))
            (if (and outer-total (plan-node-metadata-estimated-cost inner)
                     (getf relation :table) (getf relation :path))
                (values rows
                        (+ (plan-node-metadata-estimated-startup-cost outer)
                           (plan-node-metadata-estimated-startup-cost inner))
                        (+ outer-total
                           (nested-loop-inner-path-cost (getf relation :table) (getf relation :path) outer-rows)
                           (* (if cartesian-p 0.01d0 0.0125d0) candidates)))
                rows)))))))

(defun explain-scan-filter-estimates (node rows startup total relations)
  "Estimate the separate filter on a base scan and charge its CPU cost."
  (let* ((child (third node))
         (kind (first child)))
    (when (and rows total (member kind '(:seq-scan :index-scan)))
      (let* ((conjuncts (where-conjuncts (second node)))
             (indexed-p (eq kind :index-scan))
             (relation (nth (second child) relations))
             (table (getf relation :table))
             (input-rows (if (and table indexed-p (getf relation :path))
                             (select-scan-path-row-estimate table (getf relation :path))
                             rows))
             (covered-p (and indexed-p conjuncts
                             (subsetp conjuncts (where-conjuncts (fifth child)) :test #'equal)))
             (selectivity (when (and table (not covered-p))
                            (planner-filter-selectivity table conjuncts)))
             (output-rows (cond (covered-p rows)
                                (selectivity
                                 (let ((base-rows (if indexed-p (table-row-estimate table) rows)))
                                   (min rows (max 1 (round (* base-rows selectivity)))))))))
        (values output-rows startup (+ total (planner-filter-cpu-cost input-rows (length conjuncts))))))))

(defun explain-group-row-estimate (node rows relations)
  "Use the analyzed distinct count for one grouping column of a full heap scan."
  (let ((keys (second node))
        (child (fourth node)))
    (when (and rows (eq (first child) :seq-scan)
               (consp keys) (null (rest keys)) (eq (caar keys) :col))
      (let* ((table (explain-sort-input-table child relations))
             (statistics (when table (table-statistics-column table (second (first keys))))))
        (when statistics
          ;; Like estimate_num_groups in selfuncs.c, cap the number of groups
          ;; by the input rows. Native statistics count non-null values only.
          (max 1 (min rows (+ (pg-column-statistics-distinct-count statistics)
                              (if (plusp (pg-column-statistics-null-fraction statistics)) 1 0)))))))))

(defparameter +hash-aggregate-entry-overhead-bytes+ 128)

(defun hash-aggregate-entry-size (key values origins)
  "Compute the work-memory charge used by the hash aggregate."
  (+ +hash-aggregate-entry-overhead-bytes+
     (query-spill-record-size (list key values (datum-origins-for-spill origins)))))

(defparameter +hash-aggregate-partition-count+ 16)

(defun explain-hash-overflow-cost (rows groups capacity table)
  "Estimate the initial overflow file and subsequent partition passes.
Like cost_agg in costsize.c, estimate recursion from batches and fan-out.
The native executor writes one overflow file before yielding and repartitions
afterward. Assume uniform groups and use heap width as a spill-volume proxy."
  (if (>= capacity groups)
      (values 0.0d0 0.0d0)
      (let* ((overflow-groups (- groups capacity))
             (overflow-rows (* rows (/ overflow-groups groups)))
             (depth (max 1 (ceiling (log (ceiling overflow-groups capacity)
                                         +hash-aggregate-partition-count+))))
             (direction-cost (+ (ceiling (* overflow-rows (explain-heap-row-width table)) 8192)
                                (* 0.01d0 overflow-rows))))
        (values direction-cost (* 2 (1+ depth) direction-cost)))))

(defun explain-hash-aggregate-estimates (node rows total relations)
  "Cost COUNT aggregation, including modeled overflow for one integer key."
  (let* ((keys (second node))
         (groups (if keys (explain-group-row-estimate node rows relations) 1)))
    (unless (and groups rows total)
      (return-from explain-hash-aggregate-estimates groups))
    ;; cost_agg in costsize.c charges COUNT transitions and hashing
    ;; before the first output, and one tuple cost per emitted group.
    (let* ((base-start (+ total (* 0.0025d0 rows (+ (third node) (length keys)))))
           (base-total (+ base-start (* 0.01d0 groups))))
      (unless keys
        (return-from explain-hash-aggregate-estimates (values groups base-start base-total)))
      (let ((type (expr-type (first keys))))
        (if (and (member type '(:int2 :int4 :int8)) (not *datum-origin-capture*))
            (let* ((entry-size
                     (loop for value in (list (- (ash 1 63)) +null+)
                           maximize (hash-aggregate-entry-size
                                     (list (sql-group-value-key value type)) (list value) nil)))
                   (capacity (max 1 (floor (effective-query-work-memory-bytes) entry-size)))
                   (table (explain-sort-input-table (fourth node) relations)))
              (multiple-value-bind (spill-start spill-total)
                  (explain-hash-overflow-cost rows groups capacity table)
                (values groups (+ base-start spill-start) (+ base-total spill-total))))
            groups)))))

(defun explain-sorted-aggregate-estimates (node rows total relations)
  "Estimate simple integer SUM with the default transition and comparison costs."
  (let* ((keys (second node))
         (descriptors (third node))
         (groups (if keys (explain-group-row-estimate node rows relations) 1)))
    (unless (and groups rows total
                 (every (lambda (descriptor)
                          (let ((input (getf descriptor :input)))
                            (and (eq (getf descriptor :kind) :sum)
                                 (eq (first input) :col)
                                 (member (expr-type input) '(:int2 :int4)))))
                        descriptors))
      (return-from explain-sorted-aggregate-estimates groups))
    (let ((per-row (* 0.0025d0 (+ (length descriptors) (length keys))))
          (output-cost (* 0.01d0 groups)))
      (if keys
          (let ((table (explain-sort-input-table (fourth node) relations)))
            (unless table (return-from explain-sorted-aggregate-estimates groups))
            ;; cost_agg in costsize.c separates output tuple CPU from
            ;; transition and grouping work. Native grouping sorts first,
            ;; then reads through the first group before it can emit a row.
            (multiple-value-bind (sort-rows sort-start sort-total)
                (explain-sort-cost total rows rows table)
              (declare (ignore sort-rows))
              (let* ((first-group (/ rows groups))
                     (extract (/ (- sort-total sort-start) (max 1 rows)))
                     (startup (+ sort-start (* per-row first-group)
                                 (* extract (min rows (1+ first-group))))))
                (values groups startup (+ sort-total (* per-row rows) output-cost)))))
          (let ((startup (+ total (* per-row rows))))
            (values groups startup (+ startup output-cost)))))))

(defun propagate-explain-plan-estimates (plan)
  "Propagate row estimates and modeled child costs for EXPLAIN.
Projection and subquery wrappers carry the input cost until expression CPU
costs are modeled. Nodes with unmodeled work do not publish a total cost."
  (labels ((visit (node)
             (let* ((metadata (gethash node (query-plan-metadata plan)))
                    (children (mapcar #'visit (physical-plan-node-children node)))
                    (input (first children)))
               (multiple-value-bind (rows startup total)
                   (node-estimates node metadata children input)
                 (setf (plan-node-metadata-estimated-rows metadata) rows
                       (plan-node-metadata-estimated-startup-cost metadata) startup
                       (plan-node-metadata-estimated-cost metadata) total))
               metadata))
           (node-estimates (node metadata children input)
             (let ((rows (and input (plan-node-metadata-estimated-rows input)))
                   (startup (and input (plan-node-metadata-estimated-startup-cost input)))
                   (total (and input (plan-node-metadata-estimated-cost input))))
               (case (first node)
                 (:result (values 1 0.0d0 0.01d0))
                 ((:project :subquery-scan) (values rows startup total))
                 (:with-ctes rows)
                 (:filter
                  (explain-scan-filter-estimates
                   node rows startup total (getf (query-plan-prepared plan) :relations)))
                 (:hash-join
                  (explain-hash-join-estimates node input (second children)
                                               (getf (query-plan-prepared plan) :relations)))
                 (:nested-loop-join
                  (explain-nested-loop-estimates node input (second children)
                                                 (getf (query-plan-prepared plan) :relations)))
                 (:window-input
                  (if (every (lambda (key) (member (first key) '(:col :const))) (second node))
                      (values rows startup total)
                      rows))
                 (:window
                  (let* ((stage (second node))
                         (table (when (and total (getf stage :sort-required))
                                  (explain-sort-input-table
                                   (third node) (getf (query-plan-prepared plan) :relations)))))
                    (explain-window-cost stage rows startup total table)))
                 (:hash-aggregate
                  (explain-hash-aggregate-estimates
                   node rows total (getf (query-plan-prepared plan) :relations)))
                 (:sorted-aggregate
                  (explain-sorted-aggregate-estimates
                   node rows total (getf (query-plan-prepared plan) :relations)))
                 (:limit
                  (when rows
                    (let ((count (min rows (max 1 (second node)))))
                      ;; preprocess_limit floors the requested estimate at
                      ;; one. adjust_limit_rows_costs prorates the run cost.
                      (values (max 1 count) startup
                              (when total
                                (if (plusp rows)
                                    (+ startup (* (- total startup) (/ count rows)))
                                    total))))))
                 ((:sort :top-n)
                  (when rows
                    (let* ((limit (and (eq (first node) :top-n) (third node)))
                           (output-rows (if limit (max 1 (min rows limit)) rows))
                           (table (and total (not (eql limit 0))
                                       (every (lambda (key) (member (caar key) '(:col :sort-output)))
                                              (second node))
                                       (explain-sort-input-table
                                        (physical-buffered-node-child node)
                                        (getf (query-plan-prepared plan) :relations)))))
                      (if table
                          (explain-sort-cost total rows output-rows table)
                          output-rows))))
                 (:append
                  ;; cost_append uses the first child's startup cost and
                  ;; charges half the CPU tuple cost per output row.
                  (let ((output-rows (sum-child-estimates children #'plan-node-metadata-estimated-rows))
                        (child-cost (sum-child-estimates children #'plan-node-metadata-estimated-cost)))
                    (if (and output-rows child-cost)
                        (values output-rows startup (+ child-cost (* 0.005d0 output-rows)))
                        output-rows)))
                 (otherwise
                  (values (plan-node-metadata-estimated-rows metadata)
                          (plan-node-metadata-estimated-startup-cost metadata)
                          (plan-node-metadata-estimated-cost metadata))))))
           (sum-child-estimates (children accessor)
             (when (and children (every accessor children))
               (reduce #'+ children :key accessor))))
    (visit (query-plan-physical plan))))

(defun finish-query-plan (prepared registered logical physical alternatives)
  "Install one relation domain and metadata map after all child trees are rebased."
  ;; Registration IDs identify relations across local child handles; metadata
  ;; remains tied to each physical node for independent instrumentation.
  (setf registered (append registered (query-cte-relations prepared))
        logical (wrap-query-cte-plan logical prepared)
        physical (wrap-query-cte-plan physical prepared))
  (setf registered
        (mapcar (lambda (relation)
                  (unless (getf relation :registration-id)
                    (setf (getf relation :registration-id) (gensym "RELATION-")))
                  relation)
                registered))
  (setf (getf prepared :relations) registered)
  (let ((plan (%make-query-plan :logical logical :physical physical
                                :alternatives alternatives :prepared prepared)))
    (dolist (node (collect-query-plan-nodes physical))
      (let ((metadata (make-plan-node-metadata)))
        (when (member (first node) '(:seq-scan :index-scan) :test #'eq)
          (let* ((relation (nth (second node) registered))
                 (path (getf relation :path)))
            (when (and *explain-estimates-p* (not (getf relation :virtual-p)))
              (multiple-value-bind (cost rows startup)
                  (select-scan-path-cost (getf relation :table) path)
                (setf (plan-node-metadata-estimated-rows metadata) (max 1 (round rows))
                      (plan-node-metadata-estimated-cost metadata) cost
                      (plan-node-metadata-estimated-startup-cost metadata) startup)))
            (setf (plan-node-metadata-selection-rule metadata)
                  (cond ((eq (select-scan-path-kind path) :seq-scan) :sequential-fallback)
                        ((select-scan-path-range path)
                         (if (select-index-range-ordered-p (select-scan-path-range path))
                             :ordered-index :range-index))
                        ((select-scan-path-parameter-expr path) :parameterized-equality-index)
                        (t :equality-index)))))
        (setf (gethash node (query-plan-metadata plan)) metadata)))
    (when *explain-estimates-p* (propagate-explain-plan-estimates plan))
    plan))

(defun finish-select-plan-tree (input prepared physical-p &optional ordered-input-p)
  "Add final ordering and limit nodes to INPUT.
For sorted DISTINCT, omit ORDER BY keys pruned as constant from the final sort
as well. A physical ordered limit can use the bounded top-N node."
  (let ((order (unless ordered-input-p (getf prepared :order))))
    (when (and (getf prepared :distinct)
               (eq (select-distinct-plan-kind (getf prepared :targets) t) :sorted-distinct))
      (let ((keys (select-distinct-keys prepared)))
        (setf order (remove-if-not
                     (lambda (entry) (find (second (car entry)) keys :key #'second))
                     order))))
    (when order
      (setf input (list :sort order input)))
    (when (getf prepared :limit)
      (setf input
            (if (and physical-p order)
                (list :top-n order (getf prepared :limit) (third input))
                (list* :limit (getf prepared :limit) input
                       (when ordered-input-p (list (getf prepared :order)))))))
    input))

(defun plan-prepared-union (prepared)
  (let ((relations nil) (logical-arms nil) (physical-arms nil))
    (loop for arm in (list (getf prepared :left) (getf prepared :right))
          for coercions in (getf prepared :coercions)
          for child = (plan-prepared-select arm)
          for offset = (length relations)
          do (push (list coercions (rebase-select-plan-tree (query-plan-logical child) offset))
                   logical-arms)
             (push (list coercions (rebase-select-plan-tree (query-plan-physical child) offset))
                   physical-arms)
             (setf relations (append relations (getf (query-plan-prepared child) :relations))))
    (labels ((union-tree (arms physical-p)
               (let* ((arms (nreverse arms))
                      (definition (getf prepared :recursive-cte))
                      (tree (if definition
                                (list :recursive-union definition
                                      (second (first arms)) (second (second arms))
                                      (mapcar #'first arms))
                                (list :append arms))))
                 (when (and (not definition) (eq (getf prepared :set-operation) :union))
                   (setf tree (list (select-distinct-plan-kind (getf prepared :targets) physical-p)
                                    (select-distinct-keys prepared)
                                    tree)))
                 (finish-select-plan-tree tree prepared physical-p))))
      (finish-query-plan prepared relations (union-tree logical-arms nil)
                         (union-tree physical-arms t) nil))))

(defun having-gate-value (expression)
  ;; Return VALUE, KNOWN-P.  Unknown leaves never enter EVAL-EXPR.  Boolean
  ;; absorption can nevertheless establish a constant around an opaque call.
  (when (immutable-literal-expression-p expression)
    (return-from having-gate-value (values (eval-expr expression #()) t)))
  (labels ((constant (value source)
             (list :const value :type (expr-type source) :typmod (expr-typmod source)))
           (fold-operands (mapper)
             (let ((all-known-p t))
               (flet ((fold (operand)
                        (multiple-value-bind (value known) (having-gate-value operand)
                          (if known (constant value operand)
                              (progn (setf all-known-p nil) operand)))))
                 (let ((copy (funcall mapper #'fold expression)))
                   (if all-known-p (values (eval-expr copy #()) t) (values nil nil)))))))
    (case (first expression)
      (:having-opaque
       (dolist (argument (second expression)) (having-gate-value argument))
       (values nil nil))
      ((:and :or)
       (let ((and-p (eq (first expression) :and)))
         (multiple-value-bind (left left-known) (having-gate-value (second expression))
           (when (and left-known (if and-p (null left) (eq left t)))
             (return-from having-gate-value (values left t)))
           (multiple-value-bind (right right-known) (having-gate-value (third expression))
             (cond
               ((and right-known (if and-p (null right) (eq right t)))
                (values right t))
               ((and left-known right-known)
                (values (if and-p (sql-3val-and left right) (sql-3val-or left right)) t))
               (t (values nil nil)))))))
      (:coalesce
       (let ((unknown-p nil))
         (dolist (argument (second expression) (if unknown-p (values nil nil) (values +null+ t)))
           (multiple-value-bind (value known) (having-gate-value argument)
             (cond ((not known) (setf unknown-p t))
                   ((not (eq value +null+))
                    (return-from having-gate-value
                      (if unknown-p (values nil nil) (values value t)))))))))
      (:case
          (let ((unknown-p nil))
            (dolist (branch (second expression))
              (multiple-value-bind (condition known) (having-gate-value (first branch))
                (cond
                  ((not known)
                   (setf unknown-p t)
                   (having-gate-value (second branch)))
                  ((true-p condition)
                   (multiple-value-bind (value value-known) (having-gate-value (second branch))
                     (return-from having-gate-value
                       (if unknown-p (values nil nil) (values value value-known))))))))
            (multiple-value-bind (value known) (having-gate-value (third expression))
              (if unknown-p (values nil nil) (values value known)))))
      ((:not :is-null :is-bool :cast :join-relabel :unary :binop :any :all
             :concat :array-length :join-coalesce :in)
       (fold-operands #'map-local-expression-operands))
      ((:array-slice :array-subscript)
       (fold-operands #'map-analyzed-array-access-operands))
      ((:array :func)
       (fold-operands
        (lambda (fold form)
          (map-local-expression-operands (lambda (arguments) (mapcar fold arguments)) form))))
      (otherwise (values nil nil)))))

(defun having-input-gate (clauses)
  ;; Keep original WHERE and HAVING preprocessing separate after pushdown.
  ;; Fold skeletons only for queries selected for planning, preserving unused
  ;; WITH definitions.  FALSE stops an AND list; NULL does not.  The second
  ;; value identifies a qualification whose every clause folds to TRUE.
  (let ((gate nil) (all-true-p t))
    (dolist (clause clauses (values gate all-true-p))
      (multiple-value-bind (value known) (having-gate-value clause)
        (unless (and known (true-p value)) (setf all-true-p nil))
        (when (and known (not (true-p value)))
          (setf gate (list :const value :type :bool :typmod -1))
          (unless (eq value +null+)
            (return-from having-input-gate (values gate nil))))))))

(defun plan-prepared-select (prepared)
  "Build child plans without the catalog lock, then flatten their registration."
  ;; A false or null gate suppresses input; retain it above grouping when it
  ;; came from HAVING.  An all-true HAVING becomes a canonical true qual.
  (when (getf prepared :has-having)
    (let* ((where-gate (having-input-gate (getf prepared :where-gate-clauses)))
           (having-fold (multiple-value-list
                         (having-input-gate (getf prepared :having-gate-clauses))))
           (having-gate (first having-fold))
           (gate (or where-gate having-gate)))
      (when gate (setf (getf prepared :where) gate))
      (cond (having-gate (setf (getf prepared :having) having-gate))
            ((second having-fold)
             (setf (getf prepared :having) '(:bool t :type :bool :typmod -1))))))
  (when (getf prepared :deferred-window-folding)
    (unless *cte-retain-window-junk-p*
      (setf (getf prepared :window-keys)
            (loop for expression in (getf prepared :window-keys) for index from 0
                  collect (if (member index (getf prepared :window-unused-key-indices))
                              (list :const +null+ :type (expr-type expression)
                                    :typmod (expr-typmod expression))
                              expression))))
    (when *cte-retain-window-junk-p*
      (dolist (expression (getf prepared :window-validation-keys)) (fold-window-key-constant expression)))
    (setf (getf prepared :window-keys) (mapcar #'fold-window-key-constant (getf prepared :window-keys))))
  (plan-query-ctes prepared)
  (when (getf prepared :set-operation)
    (return-from plan-prepared-select (plan-prepared-union prepared)))
  (plan-expression-subqueries prepared)
  (plan-cte-reference-filters prepared)
  (dolist (relation (getf prepared :relations))
    (when (getf relation :subquery-prepared)
      (setf (getf relation :subquery-plan)
            (plan-prepared-select (getf relation :subquery-prepared)))))
  (multiple-value-bind (selected-paths alternatives) (select-plan-path-candidates prepared)
    (setf (getf prepared :relations)
          (loop for relation in (getf prepared :relations)
                for path in selected-paths for relation-index from 0
                collect (let ((copy (copy-list relation)))
                          (setf (getf copy :path) path
                                (getf copy :environment-width)
                                (select-scope-width (getf prepared :scope)))
                          (when (and (getf prepared :join-order)
                                     (= relation-index (first (getf prepared :join-order))))
                            (setf (getf copy :join-order-base) t))
                          copy)))
    (let* ((registered (flatten-select-subplans prepared))
           (logical (build-select-plan-tree prepared nil selected-paths))
           (physical (build-select-plan-tree prepared t selected-paths))
           (alternative-plans (mapcar (lambda (paths) (build-select-plan-tree prepared t paths))
                                      alternatives)))
      (finish-query-plan prepared registered logical physical alternative-plans))))

(defun sql-group-value-payload (value)
  ;; Equal values must have equal grouping keys, including values nested in
  ;; records or arrays. In particular, numeric display scale is not significant.
  ;; Check record field support before building the key, even for NULL fields.
  (cond
    ((sql-jsonb-p value) (list :jsonb (sql-jsonb-equality-key value)))
    ((sql-decimal-p value) (sql-numeric-rational value))
    ((floatp value) (sql-float-group-key value))
    ((sql-uuid-p value) (list :uuid (coerce (sql-uuid-bytes value) 'list)))
    ((sql-interval-p value) (sql-interval-comparison-value value))
    ((sql-timetz-p value) (multiple-value-list (sql-timetz-comparison-values value)))
    ((sql-array-p value)
     (list :array (sql-array-element-type-id value)
           (sql-array-dimensions value) (sql-array-lower-bounds value)
           (map 'list #'sql-group-value-payload (sql-array-elements value))))
    ((sql-record-p value)
     (list :record (mapcar #'second (sql-record-fields value))
           (loop for field in (sql-record-fields value)
                 for datum across (sql-record-values value)
                 do (check-record-field-comparison (second field) nil)
                 collect (sql-group-value-payload datum))))
    ((consp value) (cons (sql-group-value-payload (car value))
                         (sql-group-value-payload (cdr value))))
    ((and (vectorp value) (not (stringp value)))
     (map 'list #'sql-group-value-payload value))
    (t value)))

(defun sql-group-value-key (value type)
  "Build a SQL-value key; SQL NULLs compare equal for grouping."
  (if (eq value +null+)
      (list :null type)
      (list :value type
            (sql-group-value-payload value))))

(defun physical-scan-relation (node registered-relations)
  "Resolve the registered table and validate the scan node's relation identity."
  (let* ((relation-index (second node))
         (relation (and (integerp relation-index)
                        (<= 0 relation-index)
                        (< relation-index (length registered-relations))
                        (nth relation-index registered-relations)))
         (table (and relation (getf relation :table))))
    (unless (and table
                 (integerp (third node))
                 (= (third node) (pg-class-oid table)))
      (error "Physical scan relation does not match its registered table"))
    relation))

(defun physical-index-scan-parameter (node)
  (seventh node))

(defun physical-scan-path (node relation)
  "Resolve a registered scan handle and verify it matches the physical node."
  (let ((table (getf relation :table)))
    (unless (and table
                 (integerp (third node))
                 (= (third node) (pg-class-oid table)))
      (error "Physical scan relation does not match its registered table")))
  (case (first node)
    (:seq-scan (make-select-scan-path :kind :seq-scan))
    (:index-scan
     (let* ((path (getf relation :path))
            (index (and (eq (select-scan-path-kind path) :index-scan)
                        (select-scan-path-index path))))
       (unless (and index
                    (= (pg-index-oid index) (fourth node))
                    (= (pg-index-table-oid index) (third node)))
         (error "Physical IndexScan has no matching registered index"))
       (make-select-scan-path :kind :index-scan
                              :index index
                              :qual (fifth node)
                              :key (sixth node)
                              :range (select-scan-path-range path)
                              :parameter-expr (physical-index-scan-parameter node)
                              :selectivity
                              (select-scan-path-selectivity path))))))

(defun query-plan-index-relfilenodes (plan registered-relations)
  "Validate scan handles and return sorted, unique index lease relfilenodes.
Include selected scan indexes and registered TOAST indexes, signalling an error
if a registered TOAST index is missing."
  (dolist (node (collect-query-plan-nodes (query-plan-physical plan)))
    (when (eq (first node) :index-scan)
      (physical-scan-path node (physical-scan-relation node registered-relations))))
  (dolist (relation registered-relations)
    (let ((toast-nodes (getf relation :registered-toast-nodes)))
      (when (and toast-nodes (< (buf-nblocks (second toast-nodes)) 2))
        (ereport "XX001" "TOAST index is missing"))))
  (sort
   (remove-duplicates
    (loop for relation in registered-relations
          for path = (getf relation :path)
          for toast-nodes = (getf relation :registered-toast-nodes)
          append (append (when toast-nodes (list (second toast-nodes)))
                         (when (and path (eq (select-scan-path-kind path) :index-scan))
                           (list (pg-index-relfilenode (select-scan-path-index path))))))
    :test #'eql)
   #'<))

;; Plain vectors use the same representation in memory and in a spill run.
;; Terminal wire sorts can retain only projected values. Other consumers need
;; the row and its origin and presence metadata.
(defstruct (query-sort-item (:type vector)
                            (:constructor make-query-sort-item (sequence keys row)))
  sequence
  keys
  row)

(defstruct (query-sort-item-with-metadata
            (:type vector)
            (:include query-sort-item)
            (:constructor make-query-sort-item-with-metadata (sequence keys row origins presence)))
  origins
  presence)

(declaim (inline query-sort-item-key))
(defun query-sort-item-key (item entry position)
  "Get the evaluated key, or a projected output slot when keys were omitted."
  (let ((keys (query-sort-item-keys item)))
    (if keys
        (aref keys position)
        (aref (query-sort-item-row item) (1- (second (car entry)))))))

(defun select-sort-item-less-p (left right order)
  (loop for entry in order
        for position from 0
        for comparison =
                       (order-key-compare (query-sort-item-key left entry position)
                                          (query-sort-item-key right entry position)
                                          (cdr entry))
        unless (zerop comparison) return (minusp comparison)
          finally (return (< (query-sort-item-sequence left) (query-sort-item-sequence right)))))

(defun write-query-sort-run (items order)
  "Write a stable sorted run and return its path and leading key bounds.
Bounds are fixnums when available, so they remain valid after arena reset."
  (let ((sorted (stable-sort items
                             (lambda (left right)
                               (select-sort-item-less-p left right order))))
        (last-item nil))
    (with-query-temp-output (pathname output t)
      (let ((buffer (make-query-sort-spill-buffer)))
        (dolist (item sorted)
          (write-query-sort-spill-item output pathname buffer item)
          (setf last-item item)))
      ;; Only keep immediate scalar bounds, even when the run lives in an arena.
      (let ((start (and order sorted (query-sort-item-key (first sorted) (first order) 0)))
            (end (and order last-item (query-sort-item-key last-item (first order) 0))))
        (values pathname (and (typep start 'fixnum) start) (and (typep end 'fixnum) end))))))

(defun disjoint-query-sort-runs (runs descending-p)
  "Return runs in key order when their leading integer key ranges are disjoint.
Strict separation lets the caller stream whole runs without comparing rows
across runs, while preserving the order of equal keys."
  (when (every (lambda (run) (and (second run) (third run))) runs)
    (let* ((less-p (if descending-p #'> #'<))
           (ordered (stable-sort (copy-list runs) less-p :key #'second)))
      (when (loop for (run next) on ordered
                  always (or (null next) (funcall less-p (third run) (second next))))
        ordered))))

(defun make-query-sort-merge-iterator (runs order &key delete-runs-p)
  "Pull stable sort items from runs, keeping one decoded item per input.
The iterator closes its input streams. DELETE-RUNS-P also removes the run files
when the reader closes, including after an early stop."
  (let ((heap (make-array (length runs)))
        (streams nil)
        (count 0)
        (initialized nil))
    (labels ((less-p (left right)
               (select-sort-item-less-p (car left) (car right) order))
             (sift-down (position)
               (let ((entry (aref heap position)))
                 (loop for left = (1+ (* 2 position))
                       while (< left count)
                       for right = (1+ left)
                       for child = (if (and (< right count)
                                            (less-p (aref heap right) (aref heap left)))
                                       right left)
                       while (less-p (aref heap child) entry)
                       do (setf (aref heap position) (aref heap child)
                                position child))
                 (setf (aref heap position) entry)))
             (initialize ()
               (dolist (path runs)
                 (let ((input (sb-sys:without-interrupts
                                (let ((stream (open-query-temp-input path t)))
                                  (push stream streams)
                                  stream)))
                       (buffer (make-query-sort-spill-buffer)))
                   (let ((item (read-query-sort-spill-item input buffer)))
                     (unless (eq item +query-spill-eof+)
                       (setf (aref heap count) (list item input buffer))
                       (incf count)))))
               (loop for position downfrom (1- (floor count 2)) to 0
                     do (sift-down position))
               (setf initialized t))
             (advance ()
               (when (plusp count)
                 (let* ((entry (aref heap 0))
                        (next (read-query-sort-spill-item (second entry) (third entry))))
                   (if (eq next +query-spill-eof+)
                       (progn
                         (decf count)
                         (setf (aref heap 0) (aref heap count)
                               (aref heap count) nil))
                       (setf (car entry) next))
                   (when (plusp count) (sift-down 0))))))
      (make-sql-iterator
       (lambda ()
         ;; Refill only when the consumer requests another item.
         (if initialized (advance) (initialize))
         (when (plusp count)
           (values (car (aref heap 0)) t)))
       (lambda ()
         (dolist (stream streams)
           (finish-query-temp-stream stream nil))
         (when delete-runs-p
           (dolist (path runs) (delete-query-temp-file path))))))))

(defun make-disjoint-query-sort-iterator (runs)
  "Pull strictly ordered runs, reclaiming each file before opening the next."
  (let ((input nil)
        (path nil)
        (buffer (make-query-sort-spill-buffer)))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless input
           (unless runs (return))
           (sb-sys:without-interrupts
             (setf path (first (pop runs))
                   input (open-query-temp-input path t))))
         (let ((item (read-query-sort-spill-item input buffer)))
           (unless (eq item +query-spill-eof+)
             (return (values item t))))
         (finish-query-temp-stream input t)
         (delete-query-temp-file path)
         (setf input nil path nil)))
     (lambda ()
       (unwind-protect (finish-query-temp-stream input nil)
         (when path (delete-query-temp-file path))
         (dolist (run runs) (delete-query-temp-file (first run))))))))

(defun stream-query-sort-runs (runs order callback)
  (let ((iterator (make-query-sort-merge-iterator runs order)))
    (unwind-protect (map-sql-iterator iterator callback)
      (close-sql-iterator iterator))))

(defun merge-query-sort-runs (runs order)
  "Write one owned merged run before deleting its inputs.
The surrounding temporary-file scope owns all paths on an error or interrupt."
  (let ((merged
          (with-query-temp-output (pathname output t)
            (let ((buffer (make-query-sort-spill-buffer)))
              (stream-query-sort-runs
               runs order
               (lambda (item)
                 (write-query-sort-spill-item output pathname buffer item)
                 nil)))
            pathname)))
    (dolist (path runs)
      (delete-query-temp-file path))
    merged))

(defun merge-query-sort-run-pass (runs order fan-in)
  (loop while runs
        for group = (loop repeat fan-in
                          while runs collect (pop runs))
        collect (if (endp (cdr group))
                    (car group)
                    (merge-query-sort-runs group order))))

(defun grouping-rows-equal-p (keys left right)
  "Compare grouping keys in LEFT and RIGHT using SQL not-distinct semantics.
Two NULLs match, and one NULL does not. As in C ExecBuildGroupingEqual, visit
the least significant key first and stop at the first unequal key. Record
equality likewise stops at the first unequal field; an error in a later field
is raised only if comparison reaches it."
  (loop for key in (reverse keys)
        for a = (eval-expr key left)
        for b = (eval-expr key right)
        always (if (eq a +null+) (eq b +null+)
                   (and (not (eq b +null+)) (sql-compare "=" a b)))))

(defun datum-origins-for-spill (origins)
  "Represent detached origins with ordinary vectors and lists for sort/spill."
  (when origins
    (map 'vector
         (lambda (origin)
           (when origin
             (list (datum-origin-external-p origin)
                   (and (datum-origin-compressed origin)
                        (heap-compressed-varlena-payload (datum-origin-compressed origin)))
                   (datum-origin-descriptor origin))))
         origins)))

(defun datum-origins-from-spill (origins)
  "Restore owned detached origins after a row has been copied or spilled."
  (when origins
    (map 'vector
         (lambda (origin)
           (when origin
             (make-datum-origin :external-p (first origin)
                                :compressed (and (second origin)
                                                 (make-heap-compressed-varlena :payload (copy-seq (second origin))))
                                :descriptor (and (third origin) (copy-seq (third origin))))))
         origins)))

(defun top-n-spill-item (item)
  (make-query-sort-item-with-metadata (select-top-n-item-sequence item)
                                      (select-top-n-item-keys item) (select-top-n-item-row item)
                                      (datum-origins-for-spill (select-top-n-item-origins item))
                                      (select-top-n-item-presence item)))

(defun prepare-select-sort (input order arena limit projected-only-p)
  "Consume INPUT and return an iterator over sorted items.
The second value names the method selected for this invocation.
LIMIT bounds the initial top-N collector. If it exceeds the memory budget,
its retained items become sorted runs and the reader enforces the final limit.
PROJECTED-ONLY-P stores projected values without row metadata. The caller owns
the temporary-file scope for the lifetime of the result and must close the
iterator. In-memory arena items are copied out before the arena is destroyed."
  (let* ((buffer nil)
         (buffer-bytes 0)
         (max-item-bytes 1)
         (sequence 0)
         (single-order-key (and (null (cdr order)) (caar order)))
         (projected-keys-p (and projected-only-p
                                (every (lambda (entry) (eq (caar entry) :sort-output)) order)))
         (runs nil)
         (memory-limit (max 1 (effective-query-work-memory-bytes)))
         (top-n (when limit
                  (make-select-top-n-collector
                   limit order (lambda (item) (query-spill-record-size (top-n-spill-item item)))))))
    (labels ((own-run ()
               ;; Batch allocations in the sort arena to avoid switching per item.
               (when arena
                 (setf buffer (call-in-query-sort-arena
                               arena (lambda () (copy-query-sort-value buffer))))))
             (flush-run ()
               (when buffer
                 (own-run)
                 (multiple-value-bind (path start end)
                     (write-query-sort-run (nreverse buffer) order)
                   (push (list path start end) runs))
                 (setf buffer nil buffer-bytes 0)
                 (reset-query-sort-arena arena)))
             (buffer-item (item item-bytes)
               (setf max-item-bytes (max max-item-bytes item-bytes))
               (when (and buffer (> (+ buffer-bytes item-bytes) memory-limit))
                 (flush-run))
               (push item buffer)
               (incf buffer-bytes item-bytes)
               (when (>= buffer-bytes memory-limit)
                 (flush-run)))
             (make-item (row)
               (let ((sequence-number sequence)
                     (keys (unless projected-keys-p
                             (if single-order-key
                                 (vector (select-order-value single-order-key row))
                                 (map 'vector (lambda (entry) (select-order-value (car entry) row)) order)))))
                 (incf sequence)
                 (if projected-only-p
                     (make-query-sort-item sequence-number keys (car row))
                     (make-query-sort-item-with-metadata
                      sequence-number keys row
                      (and *datum-origin-capture* (datum-origins-for-spill *query-row-origins*))
                      *query-row-presence*))))
             (consume (row)
               (if top-n
                   (progn
                     (offer-select-top-n-row top-n row)
                     (when (>= (select-top-n-collector-bytes top-n) memory-limit)
                       ;; Transfer evaluated keys and tie breakers for the surviving rows.
                       (let ((heap (select-top-n-collector-heap top-n)))
                         (setf sequence (select-top-n-collector-next-sequence top-n)
                               top-n nil)
                         (loop for item across heap
                               do (buffer-item (top-n-spill-item item)
                                               (select-top-n-item-bytes item))))))
                   (let ((item (make-item row)))
                     (buffer-item item (query-spill-record-size item))))
               nil))
      (unwind-protect
           (progn
             (map-sql-iterator input #'consume)
             (when top-n
               (setf buffer (map 'list #'top-n-spill-item (select-top-n-collector-heap top-n))))
             (if (null runs)
                 (progn
                   (unless top-n (own-run))
                   (let ((items (stable-sort (nreverse buffer)
                                             (lambda (left right)
                                               (select-sort-item-less-p left right order)))))
                     (when arena (setf items (copy-query-sort-value items)))
                     (values (make-sql-iterator (lambda () (when items (values (pop items) t))))
                             (if top-n :top-n-heap :in-memory))))
                 (progn
                   (flush-run)
                   (let* ((ordered (nreverse runs))
                          (disjoint (disjoint-query-sort-runs ordered (order-descending-p (cdar order)))))
                     (if disjoint
                         (values (make-disjoint-query-sort-iterator disjoint) :external-runs)
                         (let ((paths (mapcar #'first ordered))
                               ;; Allow two inputs even if a single row exceeds work_mem.
                               (fan-in (min +query-sort-merge-fan-in+
                                            (max 2 (floor memory-limit max-item-bytes)))))
                           (loop while (> (length paths) fan-in)
                                 do (setf paths (merge-query-sort-run-pass paths order fan-in)))
                           (values (make-query-sort-merge-iterator paths order :delete-runs-p t)
                                   :external-merge)))))))
        (setf buffer nil)))))

(defparameter +hash-aggregate-max-partition-depth+ 8)

(defun hash-aggregate-group (group-keys environment)
  (let ((values nil) (origins nil))
    (dolist (expr group-keys)
      (multiple-value-bind (value origin)
          (if *datum-origin-capture*
              (eval-expr-with-origin expr environment *query-row-origins*)
              (values (eval-expr expr environment) nil))
        (push value values)
        (push origin origins)))
    (setf values (nreverse values) origins (nreverse origins))
    (values values
            (loop for expr in group-keys for value in values
                  collect (sql-group-value-key value (expr-type expr)))
            (and *datum-origin-capture* (coerce origins 'vector)))))

(defun hash-aggregate-state-row (state)
  (concatenate 'vector (car state) (cdr state)))

(defun hash-aggregate-spill-file (pathname callback)
  "Read spill rows through an owned input stream until EOF or CALLBACK stops."
  (with-query-temp-input (stream pathname)
    (loop for record = (query-spill-read-record stream)
          until (eq record +query-spill-eof+)
          do (check-for-interrupts)
             (let ((*query-row-origins* (and *datum-origin-capture*
                                             (datum-origins-from-spill (second record)))))
               (when (eq (funcall callback (if *datum-origin-capture* (first record) record)) :stop)
                 (return-from hash-aggregate-spill-file :stop)))))
  (values))

(defun hash-aggregate-spill-record (row)
  (if *datum-origin-capture*
      (list row (datum-origins-for-spill *query-row-origins*))
      row))

(defun hash-aggregate-partition-index (key depth)
  (mod (sxhash (cons depth key)) +hash-aggregate-partition-count+))

(defun hash-aggregate-partition-spill (pathname group-keys depth)
  "Partition spill rows into owned files and close outputs before returning paths."
  (let ((paths (make-array +hash-aggregate-partition-count+ :initial-element nil))
        (streams (make-array +hash-aggregate-partition-count+ :initial-element nil))
        (counts (make-array +hash-aggregate-partition-count+ :initial-element 0)))
    (declare (type simple-vector paths streams counts))
    (call-with-query-temp-stream-cleanup
     (lambda ()
       (hash-aggregate-spill-file
        pathname
        (lambda (environment)
          (multiple-value-bind (values key) (hash-aggregate-group group-keys environment)
            (declare (ignore values))
            (let* ((partition (hash-aggregate-partition-index key depth))
                   (stream (or (aref streams partition)
                               (multiple-value-bind (path output) (open-query-temp-output)
                                 (setf (aref paths partition) path
                                       (aref streams partition) output)
                                 output))))
              (query-spill-write-record stream (hash-aggregate-spill-record environment))
              (incf (aref counts partition)))))))
     (lambda (normal-p)
       (dotimes (index (length streams))
         (finish-query-temp-stream (aref streams index) normal-p))))
    (loop for index below (length paths)
          for path = (aref paths index)
          when path collect (cons (aref counts index) path))))

(defun hash-aggregate-delete-partitions (partitions)
  (dolist (partition partitions)
    (delete-query-temp-file (cdr partition)))
  (values))

(defun prepare-hash-aggregate-batch (source group-keys aggregate-count memory-limit)
  "Consume one batch and return its in-memory groups, origins and overflow file.
SOURCE is exhausted before the first group can be returned. The caller owns any
returned overflow file and processes it after the in-memory groups."
  (let ((groups (make-hash-table :test #'equal))
        (group-order nil)
        (state-origins (make-hash-table :test #'eq))
        (memory-used 0)
        (spill-path nil)
        (spill-stream nil)
        (spill-count 0))
    (labels ((make-state (values origins)
               (let ((state (cons (coerce values 'vector)
                                  (make-array aggregate-count :initial-element 0))))
                 (push state group-order)
                 (when origins (setf (gethash state state-origins) origins))
                 state))
             (start-spill ()
               (multiple-value-setq (spill-path spill-stream)
                 (open-query-temp-output)))
             (consume (environment)
               (check-for-interrupts)
               (multiple-value-bind (values key origins)
                   (hash-aggregate-group group-keys environment)
                 (let ((state (gethash key groups)))
                   (cond
                     (state
                      (dotimes (index aggregate-count)
                        (incf (aref (cdr state) index))))
                     ((and spill-stream group-keys)
                      (query-spill-write-record spill-stream (hash-aggregate-spill-record environment))
                      (incf spill-count))
                     (t
                      (let ((entry-size
                              (hash-aggregate-entry-size key values origins)))
                        (when (and group-keys group-order
                                   (> (+ memory-used entry-size) memory-limit))
                          (start-spill))
                        (if spill-stream
                            (progn
                              (query-spill-write-record spill-stream (hash-aggregate-spill-record environment))
                              (incf spill-count))
                            (let ((new-state (make-state values origins)))
                              (setf (gethash key groups) new-state)
                              (incf memory-used entry-size)
                              (dotimes (index aggregate-count)
                                (incf (aref (cdr new-state) index))))))))))))
      ;; COUNT(*) without GROUP BY has one state even when the child is empty.
      (unless group-keys
        (setf (gethash nil groups) (make-state nil nil)))
      (call-with-query-temp-stream-cleanup
       (lambda () (funcall source #'consume))
       (lambda (normal-p) (finish-query-temp-stream spill-stream normal-p)))
      (values (nreverse group-order) state-origins spill-path spill-count))))

(defun partition-hash-aggregate-overflow (path row-count group-keys depth)
  "Return shrinking partitions or indicate that this file needs a sort fallback."
  (when (>= depth +hash-aggregate-max-partition-depth+)
    (return-from partition-hash-aggregate-overflow (values nil t)))
  (let ((partitions (hash-aggregate-partition-spill path group-keys (1+ depth))))
    (if (some (lambda (partition) (>= (car partition) row-count)) partitions)
        (progn
          (hash-aggregate-delete-partitions partitions)
          (values nil t))
        (values partitions nil))))

(defstruct average-transition (sum 0) (count 0))

(defstruct interval-aggregate-transition
  (sum (make-sql-interval 0 0 0))
  (count 0)
  (positive-infinities 0)
  (negative-infinities 0)
  kind)

(defun interval-aggregate-accumulate (state value)
  "Accumulate finite intervals with checked storage arithmetic and count infinities."
  (let ((sign (sql-interval-infinity-sign value)))
    (cond ((eql sign 1) (incf (interval-aggregate-transition-positive-infinities state)))
          ((eql sign -1) (incf (interval-aggregate-transition-negative-infinities state)))
          (t (setf (interval-aggregate-transition-sum state)
                   (sql-interval-add (interval-aggregate-transition-sum state) value nil))
             (incf (interval-aggregate-transition-count state))))))

(defun interval-aggregate-result (state)
  (let ((positive (interval-aggregate-transition-positive-infinities state))
        (negative (interval-aggregate-transition-negative-infinities state))
        (count (interval-aggregate-transition-count state)))
    (cond ((and (plusp positive) (plusp negative)) (ereport "22008" "interval out of range"))
          ((plusp positive) (sql-interval-infinity 1))
          ((plusp negative) (sql-interval-infinity -1))
          ((zerop count) +null+)
          ((eq (interval-aggregate-transition-kind state) :avg)
           (sql-interval-scale (interval-aggregate-transition-sum state) (float count 1d0) t))
          (t (interval-aggregate-transition-sum state)))))

(defstruct float-average-transition
  (count 0.0d0) (sum 0.0d0) (squares 0.0d0))

(defun float-average-accumulate (state input)
  "Update the Youngs-Cramer transition used by PostgreSQL float averages."
  (let* ((value (float input 1.0d0))
         (old-count (float-average-transition-count state))
         (old-sum (float-average-transition-sum state))
         (count (+ old-count 1.0d0))
         (sum nil)
         (squares nil))
    (sb-int:with-float-traps-masked (:overflow :underflow :invalid :divide-by-zero :inexact)
      (setf sum (+ old-sum value))
      (if (plusp old-count)
          (let ((delta (- (* value count) sum)))
            (setf squares (+ (float-average-transition-squares state)
                             (/ (* delta delta) (* count old-count))))
            (when (or (sb-ext:float-infinity-p sum) (sb-ext:float-infinity-p squares))
              (unless (or (sb-ext:float-infinity-p old-sum) (sb-ext:float-infinity-p value))
                (ereport "22003" "value out of range: overflow"))
              (setf squares (sb-kernel:make-double-float #x7ff80000 0))))
          (setf squares (if (or (sb-ext:float-nan-p value) (sb-ext:float-infinity-p value))
                            (sb-kernel:make-double-float #x7ff80000 0) 0.0d0))))
    (setf (float-average-transition-count state) count
          (float-average-transition-sum state) sum
          (float-average-transition-squares state) squares)))

(defstruct sum-transition (value +null+) type)

(defun scalar-aggregate-transition-result (state)
  ;; Grouped and window aggregates share finalization: empty SUM/AVG is NULL,
  ;; exact totals are bounded at output, and AVG uses numeric division scale.
  (cond ((interval-aggregate-transition-p state) (interval-aggregate-result state))
        ((float-average-transition-p state)
         (if (zerop (float-average-transition-count state)) +null+
             (sb-int:with-float-traps-masked (:invalid :divide-by-zero :overflow :underflow :inexact)
               (/ (float-average-transition-sum state) (float-average-transition-count state)))))
        ((average-transition-p state)
         (if (zerop (average-transition-count state)) +null+
             (let ((sum (average-transition-sum state)))
               (if (sql-numeric-special-p sum) sum
                   (sql-numeric-division
                    (sql-numeric-result (sql-numeric-coefficient sum) (sql-numeric-scale sum))
                    (average-transition-count state))))))
        ((sum-transition-p state)
         (let ((value (sum-transition-value state)))
           (if (and (eq (sum-transition-type state) :numeric) (not (eq value +null+))
                    (not (sql-numeric-special-p value)))
               (let ((result (sql-numeric-result (sql-numeric-coefficient value) (sql-numeric-scale value))))
                 (if (integerp value) (sql-decimal-coefficient result) result))
               value)))
        (t state)))

(defstruct json-aggregate-transition
  ;; The spool and pending buffer hold encoded UTF-8, not input datums.  ROWS
  ;; counts inputs, not spool records; BYTES includes the opening delimiter.
  type spool (bytes 0) (rows 0) pending)

(defvar *json-aggregate-output-max-bytes* (- #x3fffffff 4))

(defun check-json-aggregate-output-size (bytes)
  (when (> bytes *json-aggregate-output-max-bytes*)
    (ereport "54000" "JSON aggregate exceeds the maximum size"))
  bytes)

(defun spool-json-aggregate-chunk (state octets)
  ;; Coalesce whole UTF-8 fragments without splitting codepoints.  Copy pending
  ;; before resetting it; an oversized chunk follows any pending data directly.
  (let ((pending (json-aggregate-transition-pending state))
        (spool (json-aggregate-transition-spool state)))
    (if (null pending)
        (query-row-spool-append spool octets)
        (progn
          (when (> (+ (length pending) (length octets)) (array-total-size pending))
            (when (plusp (length pending))
              (query-row-spool-append spool (copy-seq pending))
              (setf (fill-pointer pending) 0)))
          (if (> (length octets) (array-total-size pending))
              (query-row-spool-append spool octets)
              (let ((offset (length pending)))
                (incf (fill-pointer pending) (length octets))
                (replace pending octets :start1 offset)))))))

(defun append-json-aggregate-value (state value)
  ;; Render now to freeze session-dependent formatting and enforce the byte cap
  ;; before later inputs.  A failed transition must be closed, not resumed.
  ;; NULL inputs render as JSON null; structured types use C's newline separator.
  (let* ((spool (json-aggregate-transition-spool state))
         (type (json-aggregate-transition-type state))
         (out
           (make-instance
            'logical-utf8-output-stream
            :buffer
            (lambda (octets)
              (check-for-interrupts)
              (let ((bytes (check-json-aggregate-output-size
                            (+ (json-aggregate-transition-bytes state) (length octets)))))
                (spool-json-aggregate-chunk state octets)
                (setf (json-aggregate-transition-bytes state) bytes))))))
    (when (query-row-spool-sealed-p spool)
      (error "Cannot append to a sealed JSON aggregate"))
    (if (zerop (json-aggregate-transition-rows state))
        (write-char #\[ out)
        (progn
          (write-string ", " out)
          (when (and (not (eq value +null+))
                     (or (eq type :record) (eq type :int2vector) (sql-array-element-type type)))
            (terpri out)
            (write-char #\Space out))))
    (sql-write-json value type out)
    (logical-flush-utf8-output out)
    (incf (json-aggregate-transition-rows state))))

(defun json-aggregate-result (state &optional (seal-p t))
  ;; No inputs yield SQL NULL.  Include the closing bracket in the size check.
  ;; Group finalization seals the spool; window snapshots leave it writable and
  ;; read its prefix followed by the live pending buffer without changing either.
  (when (zerop (json-aggregate-transition-rows state))
    (return-from json-aggregate-result +null+))
  (check-json-aggregate-output-size (1+ (json-aggregate-transition-bytes state)))
  (let ((spool (json-aggregate-transition-spool state)))
    (when seal-p (query-row-spool-seal spool))
    (with-output-to-string (out)
      (map-query-row-spool-prefix
       spool
       (lambda (octets index)
         (declare (ignore index))
         (write-string (sb-ext:octets-to-string octets :external-format :utf-8) out)
         nil))
      (let ((pending (json-aggregate-transition-pending state)))
        (when (plusp (length pending))
          (write-string (sb-ext:octets-to-string pending :end (length pending)
                                                         :external-format :utf-8) out)))
      (write-char #\] out))))

(defun make-aggregate-transition-states (descriptors)
  (let ((budget (max 1 (floor (effective-query-work-memory-bytes) (max 1 (length descriptors))))))
    (map 'vector (lambda (descriptor)
                   (ecase (getf descriptor :kind)
                     ((:count :count-expr) 0)
                     ((:min :max) +null+)
                     (:sum (if (eq (getf descriptor :type) :interval)
                               (make-interval-aggregate-transition :kind :sum)
                               (make-sum-transition :type (getf descriptor :type))))
                     (:avg (case (getf descriptor :type)
                             (:interval (make-interval-aggregate-transition :kind :avg))
                             (:float8 (make-float-average-transition))
                             (otherwise (make-average-transition))))
                     (:array-agg (make-query-row-spool budget))
                     (:json-agg
                      ;; Share the budget between pending fragments and spool.
                      (let ((capacity (min 4096 (floor budget 2))))
                        (make-json-aggregate-transition
                         :type (expr-type (getf descriptor :input))
                         :spool (make-query-row-spool (- budget capacity))
                         :pending (make-array capacity :element-type '(unsigned-byte 8)
                                                       :fill-pointer 0))))))
         descriptors)))

(defun close-aggregate-transition-states (states)
  (when states
    (loop for state across states
          for spool = (cond ((query-row-spool-p state) state)
                            ((json-aggregate-transition-p state)
                             (json-aggregate-transition-spool state)))
          when spool
            do (close-query-row-spool spool)
               (when (query-row-spool-pathname spool)
                 (delete-query-temp-file (query-row-spool-pathname spool))))))

(defun sum-aggregate-transition (state value result-type)
  "Match C's NULL start, int64 int2/int4 accumulator, and exact numeric totals."
  (if (eq value +null+) state
      (let ((total (cond ((eq state +null+) value)
                         ((member result-type '(:float4 :float8)) (sql-float-arithmetic "+" state value))
                         (t (sql-numeric-accumulate state value)))))
        (if (eq result-type :int8)
            ;; numeric.c:int2_sum/int4_sum use plain int64 addition, which the
            ;; pinned C build wraps; SUM(int8/numeric) retains a wider total.
            (let ((bits (logand total #xffffffffffffffff)))
              (if (logbitp 63 bits) (- bits (ash 1 64)) bits))
            total))))

(defun step-aggregate-transition-origins (states descriptors inputs input-origins result-origins offset)
  "Keep the first MIN/MAX datum origin until another non-NULL transition."
  (when result-origins
    (loop for descriptor in descriptors for i from 0
          for value = (aref inputs (+ offset i))
          when (and (member (getf descriptor :kind) '(:min :max))
                    (not (eq value +null+)))
            do (setf (aref result-origins i)
                     (if (eq (aref states i) +null+)
                         (and input-origins (aref input-origins (+ offset i)))
                         nil)))))

(defun step-aggregate-transition-states (states descriptors inputs offset)
  (loop for descriptor in descriptors for index from 0
        for state = (aref states index)
        for value = (aref inputs (+ offset index))
        do (ecase (getf descriptor :kind)
             (:count (incf (aref states index)))
             (:count-expr (unless (eq value +null+) (incf (aref states index))))
             (:sum (if (interval-aggregate-transition-p state)
                       (unless (eq value +null+) (interval-aggregate-accumulate state value))
                       (setf (sum-transition-value state)
                             (sum-aggregate-transition (sum-transition-value state) value (getf descriptor :type)))))
             ((:min :max)
              (unless (eq value +null+)
                (when (or (eq state +null+)
                          (if (eq (getf descriptor :kind) :min)
                              (not (plusp (compare-3way value state)))
                              (not (minusp (compare-3way value state)))))
                  (setf (aref states index) value))))
             (:avg
              (unless (eq value +null+)
                (cond ((interval-aggregate-transition-p state) (interval-aggregate-accumulate state value))
                      ((float-average-transition-p state) (float-average-accumulate state value))
                      (t (setf (average-transition-sum state)
                               (sql-numeric-accumulate (average-transition-sum state) value))
                         (incf (average-transition-count state))))))
             (:array-agg (query-row-spool-append state value))
             (:json-agg (append-json-aggregate-value state value)))))

(defun aggregate-transition-results (states descriptors &optional (seal-p t))
  "Return typed group results, or snapshot appendable window transitions."
  (map 'vector
       (lambda (state descriptor)
         (cond ((json-aggregate-transition-p state) (json-aggregate-result state seal-p))
               ((query-row-spool-p state)
                (if (zerop (query-row-spool-count state)) +null+
                    (let ((result (make-array (query-row-spool-count state))))
                      (if seal-p
                          (progn
                            (query-row-spool-seal state)
                            (map-query-row-spool state (lambda (value index)
                                                         (check-for-interrupts)
                                                         (setf (aref result index) value))))
                          (map-query-row-spool-prefix
                           state (lambda (value index)
                                   (setf (aref result index) (copy-query-sort-value value)))))
                      (make-sql-array :element-type-id (sql-array-element-type (getf descriptor :type))
                                      :elements result :dimensions (list (length result))
                                      :lower-bounds (list 1)))))
               (t (scalar-aggregate-transition-result state))))
       states descriptors))

(defun aggregate-input-row (environment group-keys descriptors)
  "Evaluate inputs before spilling; replay never repeats scalar side effects."
  (let* ((expressions (append group-keys (mapcar (lambda (d) (getf d :input)) descriptors)))
         (values (make-array (length expressions) :initial-element +null+))
         (origins (and *datum-origin-capture*
                       (make-array (length expressions) :initial-element nil))))
    (loop for expression in expressions for i from 0
          when expression
            do (multiple-value-bind (value origin)
                   (if *datum-origin-capture*
                       (eval-expr-with-origin expression environment *query-row-origins*)
                       (values (eval-expr expression environment) nil))
                 (setf (aref values i) value)
                 (when origins (setf (aref origins i) origin))))
    (values values origins)))

(defun make-sql-set-function-iterator (call environment)
  (let* ((arguments (mapcar (lambda (argument) (eval-expr argument environment)) (third call)))
         (iterator
           (when (eq (first call) :set-func)
             (scalar-builtin-iterator
              (scalar-builtin-for-call (second call) arguments (first (third call)))))))
    (cond ((eq (first call) :single-record)
           (let ((value (eval-func (second call) arguments)) (pending t))
             (lambda ()
               (if pending (progn (setf pending nil) (values value t))
                   (values nil nil)))))
          (iterator (apply iterator arguments))
          (t (ereport "0A000" "unsupported set-returning function ~a" (second call))))))

(defun project-set-row-origins (origins input-width output-width)
  "Repeat input origins and clear generated or NULL-padded set-result slots."
  (when *datum-origin-capture*
    (let ((result (make-array (+ input-width output-width) :initial-element nil)))
      (when origins (replace result origins :end1 (min input-width (length origins))))
      result)))

(defun project-set-next-values (environment sets &optional attempt-function)
  "Advance each set at most once, padding exhausted calls and suppressing an all-empty row.
ATTEMPT-FUNCTION lets fused projection advance sets in target-list order."
  (let* ((width (length sets))
         (values (make-array width :initial-element +null+))
         (visited (when attempt-function (make-array width :element-type 'bit :initial-element 0)))
         (any-row-p nil))
    (flet ((step-set (index)
             (check-for-interrupts)
             (let ((state (aref sets index)))
               ;; Each slot holds its call form, its iterator, or T when exhausted.
               (when (and (not (eq state t))
                          (or (null visited) (zerop (aref visited index))))
                 (when visited (setf (aref visited index) 1))
                 (unless (functionp state)
                   (setf state (make-sql-set-function-iterator state environment)
                         (aref sets index) state))
                 (multiple-value-bind (value present-p) (funcall state)
                   (if present-p
                       (setf any-row-p t (aref values index) value)
                       (setf (aref sets index) t)))))))
      (declare (dynamic-extent #'step-set))
      (when attempt-function (funcall attempt-function environment values #'step-set))
      (dotimes (index width) (step-set index)))
    (when any-row-p values)))

(defun project-append-row (coercions environment input-origins)
  "Apply one UNION ALL arm's coercions and return its owned output row and origins."
  (let* ((width (length coercions))
         (values (make-array width))
         (origins (and *datum-origin-capture* (make-array width :initial-element nil))))
    (loop for expression in coercions for i from 0
          do (multiple-value-bind (value origin)
                 (if *datum-origin-capture*
                     (eval-expr-with-origin expression environment input-origins)
                     (values (eval-expr expression environment) nil))
               (setf (aref values i) value)
               (when origins (setf (aref origins i) origin))))
    (values (cons values values) origins)))

(defun simple-datum-origin-plan-p (node &optional relations iterator-p worktable seen)
  "Recognize plans whose execution retains detached row origins."
  (case (first node)
    ((:result :seq-scan :index-scan) t)
    ((:filter :project :project-set :limit :window-input)
     (simple-datum-origin-plan-p (third node) relations iterator-p worktable seen))
    ((:sort :top-n)
     (simple-datum-origin-plan-p
      (if (eq (first node) :top-n) (fourth node) (third node))
      relations iterator-p worktable seen))
    (:window
     (simple-datum-origin-plan-p (third node) relations iterator-p worktable seen))
    ((:hash-distinct :sorted-distinct) (simple-datum-origin-plan-p (third node) relations iterator-p worktable seen))
    (:append
     (every (lambda (arm)
              (simple-datum-origin-plan-p (second arm) relations iterator-p worktable seen))
            (second node)))
    ((:sorted-aggregate :hash-aggregate)
     (simple-datum-origin-plan-p (fourth node) relations iterator-p worktable seen))
    (:subquery-scan (simple-datum-origin-plan-p (fourth node) relations iterator-p worktable seen))
    ((:nested-loop-join :hash-join)
     (and (simple-datum-origin-plan-p (third node) relations iterator-p worktable seen)
          (simple-datum-origin-plan-p (fourth node) relations iterator-p worktable seen)))
    (:with-ctes
        (simple-datum-origin-plan-p (third node) relations iterator-p worktable seen))
    (:cte-scan
     (and relations
          (let* ((relation (physical-scan-relation node relations))
                 (definition (getf relation :cte))
                 (plan (or (getf relation :cte-reference-plan)
                           (and definition (sql-cte-plan definition)))))
            (and definition plan (not (member definition seen :test #'eq))
                 (simple-datum-origin-plan-p (query-plan-physical plan)
                                             (subquery-registered-relations plan) t worktable
                                             (cons definition seen))))))
    (:recursive-union
     (and iterator-p
          (simple-datum-origin-plan-p (third node) relations t (second node) seen)
          (simple-datum-origin-plan-p (fourth node) relations t (second node) seen)))
    (:worktable-scan
     (and iterator-p worktable relations
          (eq (getf (physical-scan-relation node relations) :cte) worktable)))
    (otherwise nil)))

(defvar *query-sort-project* nil)

(defvar *query-project-set* nil)

(defun project-set-projection-targets-p (targets width count)
  "Return true when set-dependent targets are bare ProjectSet output slots.

Expressions wrapping a set result must remain in a separate projection."
  (every (lambda (target)
           (let ((expression (getf target :expr)))
             (or (not (project-set-expression-dependent-p expression width count targets))
                 (and (eq (first expression) :col)
                      (< width (second expression))
                      (<= (second expression) (+ width count))))))
         targets))

(defun project-set-projection-node (project)
  "Return PROJECT's child when its targets can be evaluated during expansion."
  (let ((child (third project)))
    (when (and (eq (first child) :project-set)
               (integerp (fourth child))
               (not (minusp (fourth child)))
               (project-set-projection-targets-p
                (second project) (fourth child) (length (second child))))
      child)))

(defun sort-projection-volatile-p (expression)
  "Find calls whose declared volatility requires projection after sorting."
  (some-local-plan-expression
   (lambda (form walk)
     (declare (ignore walk))
     (and (eq (first form) :func)
          (eq (scalar-builtin-family-volatility (second form)) :volatile)))
   expression))

(defun project-set-expression-dependent-p (expression width count targets)
  ;; Generated slots follow WIDTH source slots. An invalid projected sort-output
  ;; reference conservatively requires expansion.
  (some-local-plan-expression
   (lambda (form walk)
     (case (first form)
       (:col (let ((slot (second form)))
               (and (integerp slot) (< width slot) (<= slot (+ width count)))))
       (:sort-output
        (let ((slot (second form)))
          (if (and (integerp slot) (<= 1 slot (length targets)))
              (funcall walk (getf (nth (1- slot) targets) :expr))
              t)))))
   expression))

(defun sort-projection-deferred-slots (node child)
  "Choose volatile targets to evaluate after sorting in either executor.

Sort keys must be computed before sorting.  Keep targets that depend on
ProjectSet output there too.  Deferred targets run only for rows delivered by
the sort or top-N, with their source datum origins."
  (when (and (member (first node) '(:sort :top-n))
             (eq (first child) :project))
    (let* ((targets (second child))
           (set-node (and (eq (first (third child)) :project-set) (third child)))
           (width (and set-node (fourth set-node)))
           (count (and set-node (length (second set-node))))
           (slots (map 'vector
                       (lambda (target)
                         (and (sort-projection-volatile-p (getf target :expr))
                              (or (null set-node)
                                  (and (integerp width)
                                       (not (project-set-expression-dependent-p
                                             (getf target :expr) width count targets))))))
                       targets)))
      (dolist (entry (second node))
        (let ((expression (car entry)))
          (when (eq (first expression) :sort-output)
            (let ((slot (second expression)))
              (unless (and (integerp slot) (<= 1 slot (length slots)))
                (return-from sort-projection-deferred-slots nil))
              (setf (aref slots (1- slot)) nil)))))
      (when (find t slots) slots))))

(defun make-sort-project-input-row (environment targets deferred)
  "Compute sort input and retain source origins for deferred targets.

The row remains (projected values . source environment).  Its private origin
sidecar contains target origins followed by source origins; ordinary sort and
spill ownership therefore cover both sets without retaining page pointers."
  (let* ((count (length targets))
         (values (make-array count :initial-element +null+))
         (source-origins *query-row-origins*)
         (origins (and *datum-origin-capture*
                       (make-array (+ count (length environment)) :initial-element nil))))
    (when (and origins source-origins)
      (replace origins source-origins :start1 count
                                      :end2 (min (length environment) (length source-origins))))
    (loop for target in targets for i from 0
          unless (aref deferred i)
            do (multiple-value-bind (value origin)
                   (if *datum-origin-capture*
                       (eval-expr-with-origin (getf target :expr) environment source-origins)
                       (values (eval-expr (getf target :expr) environment) nil))
                 (setf (aref values i) value)
                 (when origins (setf (aref origins i) origin))))
    (values (cons values environment) origins)))

(defun finish-sort-project-row (row targets deferred)
  "Evaluate deferred targets once and return the final projected origins.

The incoming private sidecar contains target origins followed by origins for
ROW's source environment.  Only final target origins leave this projection."
  (let* ((count (length targets))
         (values (copy-seq (car row)))
         (environment (cdr row))
         (packed-origins (and *datum-origin-capture* *query-row-origins*))
         (origins (and packed-origins (subseq packed-origins 0 count)))
         (source-origins (and packed-origins (subseq packed-origins count))))
    (loop for target in targets for i from 0
          when (aref deferred i)
            do (multiple-value-bind (value origin)
                   (if *datum-origin-capture*
                       (eval-expr-with-origin (getf target :expr) environment source-origins)
                       (values (eval-expr (getf target :expr) environment) nil))
                 (setf (aref values i) value)
                 (when origins (setf (aref origins i) origin))))
    (values (cons values environment) origins)))

(defun make-physical-subquery-scan-frame (node relations outer-environment)
  "Capture this scan's outer frame and the source identity of its output rows."
  (let* ((relation (physical-scan-relation node relations))
         (lateral-p (getf relation :lateral-dependent))
         (presence (vector (ensure-select-source-identity relation))))
    (values (make-subquery-parameter-frame
             (if lateral-p outer-environment #()) :capture-p lateral-p)
            presence)))

(defun project-physical-row (environment targets deferred set-node)
  "Return the projected row and its origins for either execution driver."
  (cond
    (set-node (values environment *query-row-origins*))
    (deferred (make-sort-project-input-row environment targets deferred))
    (*datum-origin-capture*
     (let ((values (make-array (length targets)))
           (origins (make-array (length targets) :initial-element nil)))
       (loop for target in targets for i from 0
             do (multiple-value-bind (value origin)
                    (eval-expr-with-origin (getf target :expr) environment *query-row-origins*)
                  (setf (aref values i) value (aref origins i) origin)))
       (values (cons values environment) origins)))
    (t (values (make-select-result-row environment nil targets) nil))))

(defun physical-buffered-node-child (node)
  "Read the child after a sort bound or aggregate specification, when present."
  (if (member (first node) '(:top-n :hash-aggregate :sorted-aggregate))
      (fourth node)
      (third node)))

(defvar *select-terminal-sort-node* nil
  "The root node of a wire SELECT that only consumes projected values.
Only this node can discard source rows and their metadata during sorting.")

(defun physical-join-rescans-inner-p (join right)
  "Recognize LATERAL and parameterized IndexScan inputs from the physical plan."
  (or (getf join :lateral-dependent)
      (and (eq (first right) :index-scan)
           (physical-index-scan-parameter right))))

(defun reset-query-plan-instrumentation (metadata-map)
  (maphash (lambda (node metadata)
             (declare (ignore node))
             (setf (plan-node-metadata-actual-row-count metadata) 0
                   (plan-node-metadata-filtered-row-count metadata) 0
                   (plan-node-metadata-resources metadata) nil
                   (plan-node-metadata-sort-methods metadata) nil
                   (plan-node-metadata-actual-loops metadata) 0
                   (plan-node-metadata-actual-startup-time-ticks metadata) 0
                   (plan-node-metadata-actual-total-time-ticks metadata) 0))
           metadata-map)
  (values))

(defun aggregate-toast-demand (node relations)
  "Prove source demand for supported single-table aggregate plans.
All group keys, aggregate inputs, filters, and index recheck expressions are
walked; unsupported expressions and plan shapes require full materialization."
  (when (eq (first node) :limit) (setf node (third node)))
  (case (first node)
    (:sort (setf node (third node)))
    (:top-n (setf node (fourth node))))
  (when (and (eq (first node) :project) (= (length relations) 1))
    (let* ((aggregate (third node))
           (groups (second aggregate))
           (specification (third aggregate)))
      (when (and (member (first aggregate) '(:hash-aggregate :sorted-aggregate))
                 (listp groups)
                 (or (and (eq (first aggregate) :hash-aggregate)
                          (integerp specification) (plusp specification))
                     (and (eq (first aggregate) :sorted-aggregate)
                          (listp specification)
                          (every (lambda (descriptor)
                                   (let ((kind (getf descriptor :kind)))
                                     (and (member kind '(:count :count-expr :sum :avg :min :max :array-agg))
                                          (or (eq kind :count) (getf descriptor :input)))))
                                 specification))))
        (let* ((inputs (if (integerp specification) nil
                           (remove nil (mapcar (lambda (descriptor) (getf descriptor :input)) specification))))
               (targets (mapcar (lambda (expr) (list :expr expr)) (append groups inputs))))
          (simple-select-toast-demand (list :project targets (fourth aggregate)) relations))))))

(defun walk-toast-demand-expression (expression demand-column mode)
  "Record known column dependencies, returning NIL if the expression needs full fetch."
  (labels ((walk (node)
             (and (consp node)
                  (case (first node)
                    ((:const :bool) t)
                    (:parameter (eq mode :update))
                    (:col (funcall demand-column (second node)))
                    (:assignment (and (eq mode :update) (walk (second node))))
                    ((:cast :not :is-null :is-bool :unary :and :or :concat :binop)
                     (every (lambda (position) (walk (nth position node)))
                            (local-expression-operand-shape (first node))))
                    (:func (and (eq mode :select) (listp (third node)) (every #'walk (third node))))
                    (otherwise nil)))))
    (declare (dynamic-extent #'walk))
    (walk expression)))

(defun simple-select-toast-demand (node relations)
  "Find demanded source columns for a simple projection, filters and ordering.
Unrecognized plans and expression forms require full materialization."
  (let ((order nil))
    (when (eq (first node) :limit) (setf node (third node)))
    (case (first node)
      (:sort (setf order (second node) node (third node)))
      (:top-n (setf order (second node) node (fourth node))))
    (when (and (eq (first node) :project) (= (length relations) 1))
      (let ((targets (second node)) (scan (third node)) (filters nil))
        (loop while (eq (first scan) :filter)
              do (push (second scan) filters)
                 (setf scan (third scan)))
        (when (and (member (first scan) '(:seq-scan :index-scan))
                   (or (eq (first scan) :seq-scan) (null (physical-index-scan-parameter scan))))
          (let* ((relation (physical-scan-relation scan relations))
                 (table (getf relation :table))
                 (slots nil))
            (labels ((demand-column (slot)
                       (when (and (integerp slot) (<= 1 slot (length (pg-class-columns table))))
                         (pushnew slot slots)
                         t))
                     (walk (expression)
                       (walk-toast-demand-expression expression #'demand-column :select))
                     (walk-order (entry)
                       (and (consp entry)
                            (let ((expr (car entry)))
                              (if (and (consp expr) (eq (first expr) :sort-output))
                                  (let ((slot (second expr)))
                                    (and (integerp slot) (<= 1 slot (length targets))))
                                  (walk expr))))))
              (when (and table (not (getf relation :row-source))
                         (not (getf relation :join-order-base))
                         (every (lambda (target) (walk (getf target :expr))) targets)
                         (every #'walk filters)
                         (every #'walk-order order)
                         (or (eq (first scan) :seq-scan)
                             (let ((path (physical-scan-path scan relation)))
                               (and (null (select-scan-path-parameter-expr path))
                                    (or (null (select-scan-path-qual path))
                                        (walk (select-scan-path-qual path)))))))
                (cons table slots)))))))))

(defun simple-select-toast-slice (node relations)
  "Recognize direct TOAST slice or length projections with no full-value uses.
Return the owning table followed by expression/slot pairs; unrecognized
projection or filter forms fall back to ordinary full materialization."
  (when (eq (first node) :limit) (setf node (third node)))
  (when (and (eq (first node) :project) (= (length relations) 1))
    (let* ((targets (second node)) (child (third node)) (scan child)
                                   (slices nil) (ordinary nil))
      (loop while (eq (first scan) :filter) do (setf scan (third scan)))
      (unless (eq (first scan) :seq-scan)
        (return-from simple-select-toast-slice nil))
      (dolist (target targets)
        (let* ((expression (getf target :expr))
               (args (and (consp expression) (eq (first expression) :func)
                          (third expression)))
               (input (first args))
               (column (if (and (consp input) (eq (first input) :cast)
                                (eq (expr-type input) :text)
                                (consp (second input))
                                (eq (first (second input)) :col)
                                (eq (expr-type (second input)) :varchar))
                           (second input) input)))
          (if (and args (consp column) (eq (first column) :col)
                   (or
                    (and (member (second expression) '("substr" "substring") :test #'string=)
                         (member (expr-type expression) '(:bytea :text)) (member (length args) '(2 3))
                         (if (eq (expr-type expression) :bytea)
                             (eq (expr-type column) :bytea)
                             (member (expr-type column) '(:text :varchar)))
                         (every (lambda (argument) (and (consp argument) (eq (first argument) :const)))
                                (rest args)))
                    (toast-length-mode expression)))
              (push (cons expression (second column)) slices)
              (push target ordinary))))
      (let ((demand (and slices
                         (simple-select-toast-demand (list :project ordinary child) relations))))
        (when (and demand
                   (every (lambda (slice)
                            (let ((slot (cdr slice)))
                              (and (integerp slot)
                                   (<= 1 slot (length (pg-class-columns (car demand)))))))
                          slices))
          (let ((eligible (remove-if (lambda (slice) (member (cdr slice) (cdr demand)))
                                     (nreverse slices))))
            (when eligible (cons (car demand) eligible))))))))

(defun select-plan-toast-access (physical relations)
  "Return the full-value demand and direct slice plan for PHYSICAL."
  (let ((slice-plan (simple-select-toast-slice physical relations))
        (demand (or (simple-select-toast-demand physical relations)
                    (aggregate-toast-demand physical relations))))
    (values (if (and slice-plan demand)
                (cons (car demand)
                      (set-difference (cdr demand) (mapcar #'cdr (cdr slice-plan))))
                demand)
            slice-plan)))

(defun physical-plan-node-children (node)
  (let ((children nil))
    (map-query-plan-children (lambda (child) (push child children)) node)
    (nreverse children)))

(defun reconcile-query-plan-access-paths (plan registered-relations)
  "Keep the physical tree in sync with admitted access paths and ordering guarantees."
  (labels ((downgrade-scan (node)
             (let* ((relation (physical-scan-relation node registered-relations))
                    (path (getf relation :path))
                    (table (getf relation :table))
                    (index (and (eq (select-scan-path-kind path) :index-scan)
                                (select-scan-path-index path))))
               (unless (and index
                            (= (pg-index-oid index) (fourth node))
                            (= (pg-index-table-oid index) (pg-class-oid table)))
                 (setf (first node) :seq-scan
                       (cdddr node) nil)
                 (let ((metadata (gethash node (query-plan-metadata plan))))
                   (when metadata
                     (setf (plan-node-metadata-selection-rule metadata) :sequential-fallback)))
                 t)))
           (visit (node)
             (when (consp node)
               (if (eq (first node) :index-scan)
                   (downgrade-scan node)
                   (let ((downgraded-p nil))
                     (dolist (child (physical-plan-node-children node))
                       (when (visit child) (setf downgraded-p t)))
                     (when (and downgraded-p (eq (first node) :limit) (fourth node))
                       ;; This LIMIT was built without a Sort because its input
                       ;; was ordered by the index. Restore top-N after DDL.
                       (setf (first node) :top-n
                             (rest node) (list (fourth node) (second node) (third node))))
                     downgraded-p)))))
    (visit (query-plan-physical plan)))
  plan)

(defun call-with-admitted-select-plan (plan function)
  "Call FUNCTION with the plan's registered relations after reconciling its access paths."
  (declare (dynamic-extent function))
  (let ((relations (getf (query-plan-prepared plan) :relations)))
    (if relations
        (with-registered-select-relations (registered relations)
          (reconcile-query-plan-access-paths plan registered)
          (funcall function registered))
        (funcall function nil))))

(defun physical-plan-node-label (node relations)
  (case (first node)
    (:seq-scan
     (let* ((relation (physical-scan-relation node relations))
            (table (getf relation :table)))
       (select-path-description table (make-select-scan-path :kind :seq-scan))))
    (:index-scan
     (let* ((relation (physical-scan-relation node relations))
            (table (getf relation :table))
            (path (physical-scan-path node relation)))
       (select-path-description table path)))
    (:result "Result")
    (:append "Append")
    (:recursive-union "Recursive Union")
    (:worktable-scan (format nil "WorkTable Scan on ~a" (sql-cte-name (getf (physical-scan-relation node relations) :cte))))
    (:subquery-scan
     (let ((relation (physical-scan-relation node relations)))
       (format nil "Subquery Scan~@[ on ~a~]" (getf relation :alias))))
    (:with-ctes "WITH")
    (:cte-scan (format nil "CTE Scan on ~a" (sql-cte-name (getf (physical-scan-relation node relations) :cte))))
    (:filter "Filter")
    (:project "Project")
    (:project-set "ProjectSet")
    (:window "WindowAgg")
    (:window-input "Window Key Projection")
    (:sort "Sort")
    (:top-n (format nil "Top-N Sort (limit ~a)" (third node)))
    (:limit "Limit")
    (:hash-aggregate "HashAggregate")
    (:sorted-aggregate "GroupAggregate")
    (:hash-distinct "HashDistinct")
    (:sorted-distinct "Unique")
    (:hash-join (format nil "Hash ~a Join" (string-capitalize (getf (second node) :type))))
    (:nested-loop-join
     (format nil "Nested Loop ~a Join"
             (case (getf (second node) :type)
               (:inner "Inner")
               (:left "Left")
               (:right "Right")
               (:full "Full")
               (:cross "Cross")
               (otherwise ""))))
    (otherwise (string-downcase (symbol-name (first node))))))

(defun plan-node-average-time-ms (ticks loops)
  (internal-time-ticks->milliseconds (/ ticks loops)))

(defun map-explain-dependent-plans (function plan relations)
  "Visit each separately planned expression subquery and CTE in display order."
  (let ((*subquery-registered-relations* (or *subquery-registered-relations* relations)))
    (labels ((visit (child relationship label)
               (let ((child-relations (subquery-registered-relations child)))
                 (reconcile-query-plan-access-paths child child-relations)
                 (funcall function child child-relations relationship label))))
      (loop for descriptor in (query-plan-expression-subplans plan)
            for number from 1
            for kind = (sql-subplan-kind descriptor)
            for relationship = (if (and (member kind '(:scalar :exists :array))
                                        (not (member 1 (sql-subplan-depths descriptor))))
                                   "InitPlan" "SubPlan")
            do (visit (sql-subplan-plan descriptor) relationship
                      (format nil "~a ~d (~a)" relationship number
                              (string-downcase (symbol-name kind)))))
      (dolist (definition (query-plan-cte-definitions plan))
        (when (sql-cte-shared-p definition)
          (visit (sql-cte-plan definition) "InitPlan"
                 (format nil "CTE ~a (shared)" (sql-cte-name definition)))))
      (dolist (node (collect-query-plan-nodes (query-plan-physical plan)))
        (when (eq (first node) :cte-scan)
          (let* ((relation (physical-scan-relation node relations))
                 (reference (getf relation :cte-reference-plan)))
            (when reference
              (visit reference "SubPlan"
                     (format nil "CTE ~a (per reference)"
                             (sql-cte-name (getf relation :cte)))))))))))

(defun explain-plan-shape-id (plan relations)
  "Identify an admitted physical tree by node kinds, scan objects and join shape."
  (labels ((node-shape (node)
             (let ((kind (first node)))
               (list kind
                     (case kind
                       (:seq-scan (third node))
                       (:index-scan (list (third node) (fourth node)))
                       (:nested-loop-join (getf (second node) :type)))
                     (mapcar #'node-shape (physical-plan-node-children node)))))
           (plan-shape (current registered)
             (let ((dependent nil))
               (map-explain-dependent-plans
                (lambda (child child-relations relationship label)
                  (declare (ignore label))
                  (push (list relationship (plan-shape child child-relations)) dependent))
                current registered)
               (list (node-shape (query-plan-physical current)) (nreverse dependent)))))
    (statement-shape-id (cons :physical-plan (plan-shape plan relations)) 1)))

(defun explain-filter-count-label (node)
  (case (first node)
    (:filter "Rows Removed by Filter")
    ((:nested-loop-join :hash-join) "Rows Removed by Join Filter")))

(defun write-explain-resource-usage (stream usage json-p)
  "Write native storage and spill counters at a query or plan-node boundary."
  (let ((reads (statement-resource-usage-relation-reads usage))
        (hits (statement-resource-usage-clean-cache-hits usage))
        (writes (statement-resource-usage-relation-writes usage))
        (spill-reads (statement-resource-usage-spill-read-bytes usage))
        (spill-writes (statement-resource-usage-spill-written-bytes usage))
        (wal-records (statement-resource-usage-wal-records usage))
        (wal-bytes (statement-resource-usage-wal-bytes usage)))
    (if json-p
        (format stream ",\"Native Relation Reads\":~d,\"Native Clean Cache Hits\":~d,~
                        \"Native Relation Writes\":~d,\"Native Spill Read Bytes\":~d,~
                        \"Native Spill Written Bytes\":~d,\"Native WAL Records\":~d,\"Native WAL Bytes\":~d"
                reads hits writes spill-reads spill-writes wal-records wal-bytes)
        (format stream
                "Native Buffers: relation reads=~d clean cache hits=~d relation writes=~d spill read bytes=~d spill written bytes=~d WAL records=~d WAL bytes=~d"
                reads hits writes spill-reads spill-writes wal-records wal-bytes))))

(defun write-explain-sort-methods (stream methods json-p)
  "Report the native sort methods seen across this node's executions."
  (flet ((label (method)
           (ecase method
             (:in-memory "in-memory stable sort")
             (:top-n-heap "top-N heap")
             (:external-runs "external run concatenation")
             (:external-merge "external merge")))
         (disk-p (method) (member method '(:external-runs :external-merge))))
    (let ((name (format nil "~{~a~^, ~}" (mapcar #'label methods)))
          (space (cond ((every #'disk-p methods) "Disk")
                       ((some #'disk-p methods) "Mixed")
                       (t "Memory"))))
      (if json-p
          (format stream ",\"Sort Method\":~s,\"Sort Space Type\":~s" name space)
          (format stream "Sort Method: ~a  Space Type: ~a" name space)))))

(defun format-query-plan (plan &optional registered-relations analyze-p
                                 execution-time nested-p)
  "Format the selected physical tree for EXPLAIN, with run stats if requested."
  (let ((relations (or registered-relations
                       (getf (query-plan-prepared plan) :relations))))
    (with-output-to-string (stream)
      (labels ((print-node (node depth)
                 (format stream "~a~a"
                         (make-string (* depth 2) :initial-element #\Space)
                         (physical-plan-node-label node relations))
                 (let* ((metadata (gethash node (query-plan-metadata plan)))
                        (estimated (and metadata (plan-node-metadata-estimated-rows metadata))))
                   (when estimated
                     (let ((cost (plan-node-metadata-estimated-cost metadata)))
                       (if cost
                           (format stream " (cost=~,2f..~,2f rows=~d)"
                                   (plan-node-metadata-estimated-startup-cost metadata) cost estimated)
                           (format stream " (rows=~d)" estimated)))))
                 (when analyze-p
                   (let* ((metadata (gethash node
                                             (query-plan-metadata plan)))
                          (loops (and metadata
                                      (plan-node-metadata-actual-loops metadata))))
                     (cond
                       ((not (and loops (plusp loops)))
                        (write-string " (never executed)" stream))
                       ((eq *query-plan-instrumentation-p* :rows)
                        (format stream " (actual rows=~,2f loops=~d)"
                                (float (plan-node-metadata-actual-rows metadata) 1.0d0)
                                loops))
                       (t
                        (format stream
                                " (actual time=~,3f..~,3f rows=~,2f loops=~d)"
                                (plan-node-average-time-ms
                                 (plan-node-metadata-actual-startup-time-ticks
                                  metadata)
                                 loops)
                                (plan-node-average-time-ms
                                 (plan-node-metadata-actual-total-time-ticks
                                  metadata)
                                 loops)
                                (float
                                 (plan-node-metadata-actual-rows metadata)
                                 1.0d0)
                                loops)))
                     (let ((label (explain-filter-count-label node)))
                       (when (and label loops (plusp loops)
                                  (plusp (plan-node-metadata-filtered-row-count metadata)))
                         (format stream " (~a: ~d)" label
                                 (round (plan-node-metadata-filtered-row-count metadata) loops))))
                     (when (and metadata (plan-node-metadata-sort-methods metadata))
                       (format stream "~%~v@T" (+ 2 (* depth 2)))
                       (write-explain-sort-methods stream (plan-node-metadata-sort-methods metadata) nil))
                     (when (and metadata (plan-node-metadata-resources metadata))
                       (format stream "~%~v@T" (+ 2 (* depth 2)))
                       (write-explain-resource-usage stream (plan-node-metadata-resources metadata) nil))))
                 (dolist (child (physical-plan-node-children node))
                   (terpri stream)
                   (print-node child (1+ depth)))))
        (print-node (query-plan-physical plan) 0)
        (map-explain-dependent-plans
         (lambda (child child-relations relationship label)
           (declare (ignore relationship))
           (format stream "~%  ~a" label)
           (dolist (line (uiop:split-string
                          (format-query-plan child child-relations analyze-p nil t)
                          :separator (string #\Newline)))
             (format stream "~%    ~a" line)))
         plan relations)
        (when (and analyze-p (not nested-p))
          (terpri stream)
          (format stream "Execution Time: ~,3f ms" (or execution-time 0.0d0)))))))

(defun select-result-column-signature (column)
  (list (result-column-name column) (result-column-table-oid column)
        (result-column-attnum column) (result-column-type-oid column)
        (result-column-typlen column) (result-column-typmod column)))

(defun check-result-column-shape (columns expected)
  "Reject a changed portal descriptor before executing its query.
Compare the advertised column shape, independently of result format codes."
  (unless (equal (mapcar #'select-result-column-signature columns)
                 (mapcar #'select-result-column-signature expected))
    (ereport "0A000" "cached plan must not change result type"))
  columns)

(defun prepare-select-result (ast &optional prepared)
  "Plan AST or its analyzed query and derive the final wire descriptor."
  (let* ((plan (if prepared (plan-prepared-select prepared) (plan-select-query ast)))
         (targets (getf (query-plan-prepared plan) :targets)))
    (values plan targets (mapcar #'target->result-column targets))))

(defun exec-perform (ast)
  "Execute a SELECT for effects and return the PL/pgSQL FOUND value."
  (multiple-value-bind (store toplevel) (statement-tracking-store)
    (multiple-value-bind (plan observation)
        (if store
            (with-statement-planning ((prepared (prepare-select ast))
                                      (query-id (statement-select-fingerprint prepared))
                                      ((plan) (plan-prepared-select prepared))
                                      ticks (statement-track-planning-p))
              (values plan (begin-planned-statement-observation
                            store toplevel query-id ticks (getf ast :normalized-statement-text))))
            (plan-select-query ast))
      (let ((rows 0))
        (flet ((discard-row (row)
                 (declare (ignore row))
                 (check-for-interrupts)
                 (incf rows)
                 nil))
          (call-with-statement-execution-slice
           observation
           (lambda ()
             (call-with-admitted-select-plan
              plan (lambda (registered)
                     (map-query-plan-rows plan registered #'discard-row))))))
        (when observation
          (setf (statement-observation-rows observation) rows)
          (finish-statement-observation observation :success))
        (plusp rows)))))

(defun copy-write-frame (stream type payload)
  "A partially transmitted COPY frame must close rather than interleave an error."
  (let ((complete nil))
    (unwind-protect
         (prog1 (write-message stream type payload) (setf complete t))
      (unless complete (ignore-errors (close stream :abort t))))))

(defun copy-text-field (text output)
  "Append a text COPY field to an octet payload.
Escape control bytes and backslashes, preserving all other bytes unchanged."
  (if (null text)
      (payload-push output (string->bytes "\\N"))
      (loop for code across (if (stringp text) (string->bytes text) text)
            for escaped = (case code
                            (8 98) (9 116) (10 110) (11 118) (12 102) (13 114) (92 92))
            do (when (zerop code) (ereport "22021" "COPY text cannot contain a zero byte"))
               (when escaped (payload-push-byte output 92))
               (payload-push-byte output (or escaped code)))))

(defun copy-text-row (values targets)
  "Build a COPY row as bytes so raw text does not pass through character encoding."
  (let ((output (make-payload)))
    (loop for value across values for target in targets for index from 0
          do (when (plusp index) (payload-push-byte output 9))
             (copy-text-field (value->text value (getf target :type)) output))
    (payload-push-byte output 10)
    output))

(defun send-copy-out-response (stream column-count)
  (let ((payload (make-payload)))
    (payload-push-byte payload 0)
    (payload-push-u16 payload column-count)
    (dotimes (index column-count) (payload-push-u16 payload 0))
    (copy-write-frame stream #\H payload)))

(defun send-copy-in-response (stream column-count)
  (let ((payload (make-payload)))
    (payload-push-byte payload 0)
    (payload-push-u16 payload column-count)
    (dotimes (index column-count) (payload-push-u16 payload 0))
    (copy-write-frame stream #\G payload)))

(defun decode-copy-text-field (bytes start end)
  "Decode one default text COPY field after recognizing its raw NULL marker."
  (when (and (= (- end start) 2) (= (aref bytes start) 92) (= (aref bytes (1+ start)) 78))
    (return-from decode-copy-text-field +null+))
  (let ((output (make-array (- end start) :element-type '(unsigned-byte 8) :fill-pointer 0)))
    (labels ((digit (byte radix)
               (digit-char-p (code-char byte) radix))
             (numeric-escape (offset radix limit)
               (loop with number = 0
                     repeat limit
                     for value = (and (< offset end) (digit (aref bytes offset) radix))
                     while value
                     do (setf number (+ (* number radix) value))
                        (incf offset)
                     finally (return (values (logand number 255) offset)))))
      (loop with offset = start
            for byte = (and (< offset end) (aref bytes offset))
            while byte
            do (incf offset)
               (when (and (= byte 92) (< offset end))
                 (setf byte (aref bytes offset))
                 (cond ((digit byte 8)
                        (multiple-value-setq (byte offset) (numeric-escape offset 8 3)))
                       ((and (= byte 120) (< (1+ offset) end)
                             (digit (aref bytes (1+ offset)) 16))
                        (multiple-value-setq (byte offset) (numeric-escape (1+ offset) 16 2)))
                       (t
                        (incf offset)
                        (setf byte (case byte
                                     (98 8) (102 12) (110 10) (114 13) (116 9) (118 11)
                                     (otherwise byte))))))
               (vector-push byte output)))
    (when (find 0 output)
      (ereport "22021" "null byte in COPY field"))
    (handler-case (bytes->string output)
      (sb-int:character-decoding-error ()
        (ereport "22021" "invalid UTF-8 in COPY field")))))

(defun decode-copy-text-row (bytes column-count)
  "Split unescaped tabs before decoding fields so an escaped tab stays in its field."
  (when (and (zerop column-count) (zerop (length bytes)))
    (return-from decode-copy-text-row nil))
  (let ((start 0) (escaped-p nil) (fields nil))
    (loop for byte across bytes for offset from 0
          do (cond (escaped-p (setf escaped-p nil))
                   ((= byte 92) (setf escaped-p t))
                   ((= byte 9)
                    (push (decode-copy-text-field bytes start offset) fields)
                    (setf start (1+ offset)))))
    (push (decode-copy-text-field bytes start (length bytes)) fields)
    (unless (= (length fields) column-count)
      (ereport "22P04" "COPY expected ~d fields, got ~d" column-count (length fields)))
    (nreverse fields)))

(defun send-copy-command-complete (stream count)
  (send-command-complete stream (format nil "COPY ~d" count)))

(defun map-copy-in-text-rows (stream column-count function)
  "Read default text COPY to CopyDone, retaining at most one partial row."
  (let ((line (make-payload))
        (escaped-p nil)
        (pending-cr-p nil)
        (line-ending nil)
        (end-marker-p nil))
    (labels ((consume-line (&optional terminator)
               (when terminator
                 (when (and line-ending (not (eq terminator line-ending)))
                   (ereport "22P04" "inconsistent line endings in COPY data"))
                 (setf line-ending terminator))
               (cond ((and (= (length line) 2) (= (aref line 0) 92) (= (aref line 1) 46))
                      (setf end-marker-p t))
                     (t (funcall function (decode-copy-text-row line column-count))))
               (setf (fill-pointer line) 0 escaped-p nil))
             (consume-byte (byte)
               ;; An unescaped CR can be the start of CRLF in the next frame.
               (when pending-cr-p
                 (setf pending-cr-p nil)
                 (let ((lf-p (= byte 10)))
                   (consume-line (if lf-p :crlf :cr))
                   (when (or lf-p end-marker-p)
                     (return-from consume-byte))))
               (cond ((and (= byte 13) (not escaped-p))
                      (setf pending-cr-p t))
                     ((and (= byte 10) (not escaped-p))
                      (consume-line :lf))
                     (t
                      (when (>= (length line) +max-query-message-size+)
                        (ereport "54000" "COPY row exceeds the size limit"))
                      (vector-push-extend byte line)
                      (setf escaped-p (and (= byte 92) (not escaped-p)))))))
      (loop
        (check-for-interrupts)
        (multiple-value-bind (type length payload) (read-message-frame stream)
          (unless type (ereport-fatal "08006" "connection ended during COPY"))
          (decode-frontend-message type length payload)
          (case (code-char type)
            (#\d
             (loop for byte across payload until end-marker-p do (consume-byte byte)))
            (#\c
             (unless (zerop (length payload)) (malformed-protocol-message "CopyDone has a payload"))
             (cond (pending-cr-p (consume-line :cr))
                   ((plusp (length line)) (consume-line)))
             (return))
            (#\f (ereport "57014" "COPY from stdin failed"))
            (#\X (ereport-fatal "08006" "connection terminated during COPY"))
            ((#\H #\S) nil)
            (otherwise (malformed-protocol-message "unexpected frontend message during COPY"))))))))

(defun copy-from-target (ast)
  "Resolve the target and explicit input order without starting COPY."
  (with-write-lock
    (let* ((table (resolve-table-or-error (getf ast :table)))
           (columns (pg-class-columns table))
           (attnums (if (getf ast :columns)
                        (resolve-attnums table (getf ast :columns))
                        (loop for column across columns
                              unless (pg-column-dropped-p column)
                                collect (pg-column-attnum column)))))
      (values table columns attnums))))

(defun exec-copy-from (stream ast)
  "Import text rows under one command and validate references before reporting success."
  (multiple-value-bind (table columns attnums) (copy-from-target ast)
    (let* ((prepared (prepare-dml-column-defaults
                      (list :dml-type :insert :table table :columns columns :attnums attnums)
                      (list :type :insert)))
           (references nil)
           (count 0))
      (call-with-dml-subqueries
       prepared
       (lambda (table node xid)
         (declare (ignore node xid))
         (let ((child-probes (prepare-child-foreign-key-probes table)))
           (send-copy-in-response stream (length attnums))
           (map-copy-in-text-rows
            stream (length attnums)
            (lambda (fields)
              (check-for-interrupts)
              (let ((row (make-array (length columns) :initial-element +null+)))
                (loop for column across columns for attnum from 1
                      for supplied = (member attnum attnums)
                      unless (or supplied (pg-column-dropped-p column))
                        do (setf (aref row (1- attnum)) (coerce-dml-column-default column)))
                (loop for attnum in attnums for value in fields
                      for column = (aref columns (1- attnum))
                      do (setf (aref row (1- attnum))
                               (if (eq value +null+) +null+
                                   (coerce-dml-assignment-value value :text (pg-column-type column)
                                                                (pg-column-typmod column) t))))
                (check-not-null row columns)
                (check-table-constraints table row)
                (multiple-value-bind (block off publications)
                    (insert-tuple-with-unique table row :publication-kind :insert)
                  (logical-note-row-change :insert table :new-values row :publication-selection publications)
                  (when child-probes
                    (setf references (nconc (lock-foreign-key-child-references
                                             table child-probes row (list block off (backend-command-id *backend*)))
                                            references))))
                (incf count))))
           (check-foreign-key-child-references table references))))
      count)))

(defun exec-copy-to (stream ast)
  "Stream a SELECT plan's snapshot rows using C's default text COPY format."
  (let* ((plan (plan-select-query (getf ast :query)))
         (prepared (query-plan-prepared plan))
         (targets (getf prepared :targets))
         (count 0))
    (labels ((send-row (row)
               (check-for-interrupts)
               (copy-write-frame stream #\d (copy-text-row (car row) targets))
               (incf count))
             (copy-plan (registered)
               (send-copy-out-response stream (length targets))
               (let ((source (getf (getf ast :query) :from)))
                 (logical-test-failpoint
                  :after-copy-out-response
                  (when (sql-relation-name-p source) (sql-relation-name-name source))))
               (map-query-plan-rows plan registered #'send-row)))
      (call-with-admitted-select-plan plan #'copy-plan)
      (copy-write-frame stream #\c #())
      count)))

(defun write-explain-json-node-identity (stream node relations)
  "Write structural scan names separately from their catalog object names."
  (let ((kind (first node)))
    (case kind
      ((:seq-scan :index-scan)
       (let* ((relation (physical-scan-relation node relations))
              (table (getf relation :table))
              (index-p (eq kind :index-scan)))
         (format stream "~s:~s,~s:~a"
                 "Node Type" (if index-p "Index Scan" "Seq Scan")
                 "Relation Name" (sql-typed-to-json :text (pg-class-name table)))
         (when index-p
           (format stream ",~s:~s" "Scan Direction"
                   (if (eq (eighth node) :backward) "Backward" "Forward"))
           (format stream ",~s:~a" "Index Name"
                   (sql-typed-to-json
                    :text (pg-index-name (select-scan-path-index
                                          (physical-scan-path node relation))))))))
      ((:cte-scan :worktable-scan :subquery-scan)
       (let* ((relation (physical-scan-relation node relations))
              (node-type (ecase kind
                           (:cte-scan "CTE Scan")
                           (:worktable-scan "WorkTable Scan")
                           (:subquery-scan "Subquery Scan")))
              (cte (getf relation :cte))
              (alias (getf relation :alias)))
         (format stream "~s:~s" "Node Type" node-type)
         (when cte
           (format stream ",~s:~a" "CTE Name" (sql-typed-to-json :text (sql-cte-name cte))))
         (when alias
           (format stream ",~s:~a" "Alias" (sql-typed-to-json :text alias)))))
      (:hash-join (format stream "~s:~s,~s:~s,~s:~s" "Node Type" "Hash Join" "Join Type"
                          (string-capitalize (getf (second node) :type))
                          "Build Side" (if (getf (second node) :build-left-p) "Left" "Right")))
      (:nested-loop-join
       (let ((join-type (getf (second node) :type)))
         (format stream "~s:~s,~s:~s" "Node Type" "Nested Loop" "Join Type"
                 (if (eq join-type :cross) "Inner" (string-capitalize (symbol-name join-type))))))
      (otherwise
       (format stream "~s:~a" "Node Type"
               (sql-typed-to-json :text (physical-plan-node-label node relations)))))))

(defun format-query-plan-json (plan relations analyze-p timing-p execution-time &optional planning-time resources query-id temp-peak)
  "Encode the selected physical plan and its available execution statistics."
  (with-output-to-string (stream)
    (labels ((write-node (node plan relations &optional root-p relationship label)
               (write-char #\{ stream)
               (write-explain-json-node-identity stream node relations)
               (when relationship
                 (format stream ",~s:~a" "Parent Relationship" (sql-typed-to-json :text relationship)))
               (when label
                 (format stream ",~s:~a" "Subplan Name" (sql-typed-to-json :text label)))
               (let* ((metadata (gethash node (query-plan-metadata plan)))
                      (estimated (and metadata (plan-node-metadata-estimated-rows metadata))))
                 (when estimated
                   (let ((cost (plan-node-metadata-estimated-cost metadata)))
                     (when cost
                       (format stream ",\"Startup Cost\":~,2f,\"Total Cost\":~,2f"
                               (plan-node-metadata-estimated-startup-cost metadata) cost)))
                   (format stream ",\"Plan Rows\":~d" estimated)))
               (when analyze-p
                 (let* ((metadata (gethash node (query-plan-metadata plan)))
                        (loops (if metadata (plan-node-metadata-actual-loops metadata) 0))
                        (rows (if (plusp loops) (plan-node-metadata-actual-rows metadata) 0)))
                   (format stream ",~s:~,2f,~s:~d" "Actual Rows" (float rows 1.0d0)
                           "Actual Loops" loops)
                   ;; Native B-tree quals are exact. Heap visibility skips are
                   ;; not failures of an index predicate recheck.
                   (when (and (eq (first node) :index-scan) (fifth node))
                     (write-string ",\"Rows Removed by Index Recheck\":0" stream))
                   (let ((label (explain-filter-count-label node)))
                     (when label
                       (format stream ",~s:~d" label
                               (if (plusp loops)
                                   (round (plan-node-metadata-filtered-row-count metadata) loops)
                                   0))))
                   (when (and metadata (plan-node-metadata-sort-methods metadata))
                     (write-explain-sort-methods stream (plan-node-metadata-sort-methods metadata) t))
                   (when (and metadata (plan-node-metadata-resources metadata))
                     (write-explain-resource-usage stream (plan-node-metadata-resources metadata) t))
                   (when (and metadata (plan-node-metadata-hash-join-stats metadata))
                     (let ((stats (plan-node-metadata-hash-join-stats metadata)))
                       (let ((probes (hash-join-stats-key-probes stats)))
                         (format stream ",~s:~d"
                                 (if probes "Native Hash Key Probes" "Native Hash Candidates")
                                 (or probes (hash-join-stats-candidates stats))))
                       (format stream ",~s:~d,~s:~d,~s:~d"
                               "Native Hash Spill Batches" (hash-join-stats-spill-batches stats)
                               "Native Hash Build Blocks" (hash-join-stats-build-blocks stats)
                               "Native Hash Peak Build Bytes" (hash-join-stats-peak-build-bytes stats))))
                   (when timing-p
                     (format stream ",~s:~,3f,~s:~,3f"
                             "Actual Startup Time"
                             (if (plusp loops)
                                 (plan-node-average-time-ms
                                  (plan-node-metadata-actual-startup-time-ticks metadata) loops)
                                 0.0d0)
                             "Actual Total Time"
                             (if (plusp loops)
                                 (plan-node-average-time-ms
                                  (plan-node-metadata-actual-total-time-ticks metadata) loops)
                                 0.0d0)))))
               (let ((opened-p nil))
                 (flet ((write-child (child child-plan child-relations
                                      &optional child-root-p child-relationship child-label)
                          (if opened-p
                              (write-char #\, stream)
                              (progn
                                (format stream ",~s:[" "Plans")
                                (setf opened-p t)))
                          (write-node child child-plan child-relations
                                      child-root-p child-relationship child-label)))
                   (loop for child in (physical-plan-node-children node)
                         for position from 0
                         for role = (cond ((eq (first node) :append) "Member")
                                          ((eq (first node) :subquery-scan) "Subquery")
                                          ((zerop position) "Outer")
                                          ((= position 1) "Inner"))
                         do (write-child child plan relations nil role))
                   (when root-p
                     (map-explain-dependent-plans
                      (lambda (child-plan child-relations child-relationship child-label)
                        (write-child (query-plan-physical child-plan) child-plan child-relations
                                     t child-relationship child-label))
                      plan relations)))
                 (when opened-p (write-char #\] stream)))
               (write-char #\} stream)))
      (format stream "[{~s:" "Plan")
      (write-node (query-plan-physical plan) plan relations t)
      (when query-id
        (format stream ",\"Query Identifier\":~d,\"Plan Identifier\":~d"
                query-id (explain-plan-shape-id plan relations)))
      (when planning-time
        (format stream ",~s:~,3f" "Planning Time" planning-time))
      (when analyze-p
        (format stream ",~s:~,3f" "Execution Time" execution-time))
      (when resources
        (write-explain-resource-usage stream resources t))
      (when temp-peak
        (format stream ",\"Native Peak Temp Bytes\":~d" temp-peak))
      (write-string "}]" stream))))

(defun exec-explain (stream ast)
  (let* ((analyze-p (getf ast :analyze-p))
         (timing-p (getf ast :timing-p t))
         (json-p (eq (getf ast :format) :json))
         (planning-start (when (and analyze-p json-p) (get-internal-real-time)))
         (resources (when (getf ast :buffers-p) (make-statement-resource-usage)))
         (query-id nil)
         (plan (let ((*statement-resource-usage* (if analyze-p *statement-resource-usage* resources))
                     (*explain-estimates-p* (getf ast :costs-p json-p)))
                 (if (and json-p *statement-statistics-store*)
                     (let ((prepared (prepare-select (getf ast :statement))))
                       (setf query-id (statement-select-fingerprint prepared))
                       (plan-prepared-select prepared))
                     (plan-select-query (getf ast :statement)))))
         (planning-time (when planning-start
                          (internal-time-ticks->milliseconds
                           (- (get-internal-real-time) planning-start))))
         (*query-plan-instrumentation-p* (and analyze-p (if timing-p t :rows)))
         (description nil)
         (temp-peak nil)
         (execution-time 0.0d0))
    (labels ((prepare-explain (registered)
               (when analyze-p
                 (let ((started (get-internal-real-time))
                       (*statement-resource-usage* (or resources *statement-resource-usage*))
                       (*query-temp-space-sample* (when resources (make-query-temp-space-sample))))
                   (map-query-plan-rows
                    plan registered
                    (lambda (row)
                      (declare (ignore row))
                      nil)
                    :projected-only-p t)
                   (setf execution-time
                         (internal-time-ticks->milliseconds
                          (- (get-internal-real-time) started))
                         temp-peak (when *query-temp-space-sample*
                                     (query-temp-space-sample-peak-bytes *query-temp-space-sample*)))))
               (setf description
                     (if json-p
                         (format-query-plan-json plan registered analyze-p timing-p
                                                 execution-time planning-time resources query-id temp-peak)
                         (let ((text (format-query-plan plan registered analyze-p execution-time)))
                           (if resources
                               (with-output-to-string (output)
                                 (write-line text output)
                                 (write-explain-resource-usage output resources nil)
                                 (when temp-peak
                                   (format output "~%Native Peak Temp Bytes: ~d" temp-peak)))
                               text))))))
      (call-with-admitted-select-plan plan #'prepare-explain))
    (send-row-description
     stream
     (list (make-result-column :name "QUERY PLAN"
                               :type-oid (if json-p (type-oid :json) +textoid+)
                               :typlen -1
                               :typmod -1)))
    (send-data-row-text stream (list description))
    (send-command-complete stream "EXPLAIN")))
