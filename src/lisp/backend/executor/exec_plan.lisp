;;;; exec_plan.lisp -- S-expression query plans and their reference interpreter.

(in-package #:pg)

(defstruct plan-node-metadata
  estimated-rows
  estimated-cost
  actual-rows
  (actual-row-count 0)
  (actual-loops 0)
  (actual-startup-time-ticks 0)
  (actual-total-time-ticks 0)
  selection-rule)

(defstruct (query-plan (:constructor %make-query-plan))
  logical
  physical
  alternatives
  prepared
  (metadata (make-hash-table :test #'eq)))

(defstruct query-plan-backend
  executor
  compiler)

(defvar *query-plan-backends* (make-hash-table :test #'eq))
(defvar *query-plan-instrumentation-p* nil)
(defvar *query-plan-backend-lock*
  (sb-thread:make-mutex :name "pg query-plan backend registry"))

(defun internal-time-ticks->milliseconds (ticks)
  (* 1000.0d0 (/ ticks internal-time-units-per-second)))

(defun query-plan-backend-entry (name)
  (sb-thread:with-mutex (*query-plan-backend-lock*)
    (gethash name *query-plan-backends*)))

(defun register-query-plan-backend (name &key executor compiler)
  "Register an executor and/or compiler for physical query plans.

An executor accepts PLAN, REGISTERED-RELATIONS and ROW-CALLBACK. A compiler
accepts PLAN and returns a function accepting REGISTERED-RELATIONS and
ROW-CALLBACK."
  (unless (or executor compiler)
    (error "A query-plan backend needs an executor or compiler"))
  (let ((entry (make-query-plan-backend :executor executor :compiler compiler)))
    (sb-thread:with-mutex (*query-plan-backend-lock*)
      (setf (gethash name *query-plan-backends*) entry)))
  name)

(defun compile-query-plan (plan &optional (backend :sbcl-compiled))
  "Compile PLAN through BACKEND's compiler hook, if one is installed."
  (let* ((entry (query-plan-backend-entry backend))
         (compiler (and entry (query-plan-backend-compiler entry))))
    (unless compiler
      (ereport "0A000" "query-plan compiler backend ~a is not installed"
               backend))
    (funcall compiler plan)))

(defun execute-query-plan (plan registered-relations row-callback &key (backend :interpreter))
  "Execute a plan and its subqueries in one registered statement domain."
  (let* ((root-p (not *subquery-execution-active-p*))
         (*subquery-result-cache* (if root-p (make-hash-table :test #'eq) *subquery-result-cache*))
         (*subquery-registered-relations* (if root-p registered-relations *subquery-registered-relations*))
         (*subquery-outer-frames* (if root-p nil *subquery-outer-frames*))
         (*subquery-backend-entry* (if root-p (query-plan-backend-entry backend) *subquery-backend-entry*))
         (*subquery-execution-active-p* t)
         (entry *subquery-backend-entry*)
         (executor (and entry (query-plan-backend-executor entry))))
    (call-with-query-cte-execution
     root-p
     (lambda ()
       (call-with-select-index-lease-domain
        (query-plan-index-relfilenodes plan registered-relations)
        (lambda ()
          (cond
            (executor (funcall executor plan registered-relations row-callback))
            ((and entry (query-plan-backend-compiler entry))
             (funcall (funcall (query-plan-backend-compiler entry) plan) registered-relations row-callback))
            (t (ereport "0A000" "query-plan backend ~a is not installed" backend)))))))))

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
PostgreSQL's full join-relation graph. Join quals are applied above the
reordered tree, and each parameterized path can reference only joined Vars."
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
                      (choose-select-scan-path table (getf prepared :where)))))
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
       (list :index-scan relation-index (pg-class-oid table)
             (pg-index-oid (select-scan-path-index path))
             (select-scan-path-qual path) (select-scan-path-key path)
             (select-scan-path-parameter-expr path)))
      (t (list :seq-scan relation-index (pg-class-oid table))))))

(defun select-plan-input-form (prepared physical-p paths)
  (let* ((relations (getf prepared :relations))
         (joins (getf prepared :joins))
         (join-order (getf prepared :join-order)))
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

(defun prepared-transition-aggregate-p (prepared)
  "Only COUNT(*) can use the count-only hash aggregate fast path."
  (some (lambda (descriptor) (not (eq (getf descriptor :kind) :count)))
        (getf prepared :aggregates)))

(defun build-select-plan-tree (prepared physical-p paths)
  (let* ((input (select-plan-input-form prepared physical-p paths))
         (where (getf prepared :where))
         (aggregate-p (getf prepared :aggregate-p))
         (degenerate-having-p (and (getf prepared :has-having)
                                   (not (getf prepared :group-keys))
                                   (zerop (getf prepared :aggregate-count 0)))))
    (when (getf prepared :join-order)
      (let ((join-qual (select-reordered-join-qual prepared)))
        (when join-qual (setf input (list :filter join-qual input)))))
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
                      (if (prepared-transition-aggregate-p prepared) :sorted-aggregate :hash-aggregate)
                      :aggregate)
                  (getf prepared :group-keys)
                  (if (prepared-transition-aggregate-p prepared)
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
    (when (getf prepared :set-functions)
      (setf input (list :project-set (getf prepared :set-functions) input)))
    (setf input (list :project (getf prepared :targets) input))
    (when (getf prepared :distinct)
      (setf input (list (if physical-p :hash-distinct :distinct)
                        (loop for target in (getf prepared :targets) for position from 1
                              collect (projected-target-ref target position))
                        input)))
    (finish-select-plan-tree input prepared physical-p)))

(defun query-plan-relation-node-p (node)
  "Relation handles occupy slot two; rebasing must cover every such IR node."
  (and (consp node)
       (member (first node) '(:scan :seq-scan :index-scan :subquery-scan :cte-scan :worktable-scan))))

(defun query-plan-operator-p (node)
  (and (consp node)
       (or (query-plan-relation-node-p node)
           (member (first node)
                   '(:recursive-union :with-ctes :window-input :window :append :result :filter :join :nested-loop-join :project :aggregate
                     :sort :top-n :limit :project-set :hash-aggregate :sorted-aggregate :distinct :hash-distinct)
                   :test #'eq))))

(defun collect-query-plan-nodes (tree)
  (let ((nodes '()))
    (labels ((visit (form)
               (when (consp form)
                 (when (query-plan-operator-p form)
                   (push form nodes))
                 (visit (car form))
                 (visit (cdr form)))))
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
            (setf (plan-node-metadata-selection-rule metadata)
                  (if (eq (select-scan-path-kind path) :index-scan)
                      (if (select-scan-path-parameter-expr path)
                          :parameterized-equality-index :equality-index)
                      :sequential-fallback))))
        (setf (gethash node (query-plan-metadata plan)) metadata)))
    plan))

(defun finish-select-plan-tree (input prepared physical-p)
  (when (getf prepared :order)
    (setf input (list :sort (getf prepared :order) input)))
  (when (getf prepared :limit)
    (setf input
          (if (and physical-p (getf prepared :order))
              (list :top-n (getf prepared :order) (getf prepared :limit) (third input))
              (list :limit (getf prepared :limit) input))))
  input)

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
                   (setf tree (list (if physical-p :hash-distinct :distinct)
                                    (loop for target in (getf prepared :targets) for slot from 1
                                          collect (projected-target-ref target slot))
                                    tree)))
                 (finish-select-plan-tree tree prepared physical-p))))
      (finish-query-plan prepared relations (union-tree logical-arms nil)
                         (union-tree physical-arms t) nil))))

(defun having-gate-value (expression)
  ;; Return VALUE, KNOWN-P.  Unknown leaves never enter EVAL-EXPR.  Boolean
  ;; absorption can nevertheless establish a constant around an opaque call.
  (when (having-literal-expression-p expression)
    (return-from having-gate-value (values (eval-expr expression #()) t)))
  (labels ((constant (value source)
             (list :const value :type (expr-type source) :typmod (expr-typmod source)))
           (fold-children (positions)
             (let ((copy (copy-list expression)) (known-p t))
               (dolist (position positions)
                 (multiple-value-bind (value known) (having-gate-value (nth position expression))
                   (if known
                       (setf (nth position copy) (constant value (nth position expression)))
                       (setf known-p nil))))
               (if known-p (values (eval-expr copy #()) t) (values nil nil)))))
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
      ((:not :is-null :is-bool :cast :join-relabel) (fold-children '(1)))
      (:unary (fold-children '(2)))
      ((:binop :any :all) (fold-children '(2 3)))
      ((:concat :array-length :join-coalesce) (fold-children '(1 2)))
      (:array-slice
       (let ((copy (copy-list expression)) (known-p t))
         (labels ((fold-bound (bound)
                    (when bound
                      (multiple-value-bind (value known) (having-gate-value bound)
                        (unless known (setf known-p nil))
                        (if known (constant value bound) bound)))))
           (let* ((pairs (third expression))
                  (uppers (mapcar (lambda (pair) (fold-bound (second pair))) pairs))
                  (lowers (mapcar (lambda (pair) (fold-bound (first pair))) pairs)))
             (setf (third copy) (mapcar #'list lowers uppers)
                   (second copy) (fold-bound (second expression))))
           (if known-p (values (eval-expr copy #()) t) (values nil nil)))))
      (:array-subscript
       (let ((copy (copy-list expression)) (known-p t))
         (labels ((fold-part (part)
                    (multiple-value-bind (value known) (having-gate-value part)
                      (unless known (setf known-p nil))
                      (if known (constant value part) part))))
           (setf (third copy) (mapcar #'fold-part (third expression))
                 (second copy) (fold-part (second expression)))
           (if known-p (values (eval-expr copy #()) t) (values nil nil)))))
      (:in
       (let ((copy (copy-list expression)) (known-p t))
         (labels ((fold (part)
                    (multiple-value-bind (value known) (having-gate-value part)
                      (if known (constant value part)
                          (progn (setf known-p nil) part)))))
           ;; Copy the metadata tail before replacing its analyzed operands.
           (setf (cddddr copy) (copy-list (cddddr expression))
                 (second copy) (fold (second expression))
                 (third copy) (mapcar #'fold (third expression))
                 (getf (cddddr copy) :array-left)
                 (when (getf (cddddr expression) :array-left)
                   (fold (getf (cddddr expression) :array-left)))
                 (getf (cddddr copy) :array-items)
                 (mapcar #'fold (getf (cddddr expression) :array-items))
                 (getf (cddddr copy) :comparisons)
                 (mapcar (lambda (pair) (mapcar #'fold pair))
                         (getf (cddddr expression) :comparisons)))
           (if known-p (values (eval-expr copy #()) t) (values nil nil)))))
      ((:array :func)
       (let* ((position (if (eq (first expression) :func) 2 1))
              (arguments (nth position expression))
              (copy (copy-list expression))
              (known-p t)
              (folded nil))
         (dolist (argument arguments)
           (multiple-value-bind (value known) (having-gate-value argument)
             (if known (push (constant value argument) folded)
                 (setf known-p nil))))
         (if known-p
             (progn (setf (nth position copy) (nreverse folded))
                    (values (eval-expr copy #()) t))
             (values nil nil))))
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
  ;; Equal numeric values group together regardless of their display scale,
  ;; including when nested in records or arrays.
  (cond
    ((sql-jsonb-p value) (list :jsonb (sql-jsonb-equality-key value)))
    ((sql-decimal-p value) (sql-numeric-rational value))
    ((floatp value) (sql-float-group-key value))
    ((sql-interval-p value) (sql-interval-comparison-value value))
    ((sql-timetz-p value) (multiple-value-list (sql-timetz-comparison-values value)))
    ((sql-array-p value)
     (list :array (sql-array-element-type-id value)
           (sql-array-dimensions value) (sql-array-lower-bounds value)
           (map 'list #'sql-group-value-payload (sql-array-elements value))))
    ((sql-record-p value)
     (list :record (mapcar #'second (sql-record-fields value))
           (map 'list #'sql-group-value-payload (sql-record-values value))))
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
                              :parameter-expr (seventh node)
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

(defun select-sort-item-less-p (left right order)
  (loop for entry in order
        for position from 0
        for comparison =
                       (order-compare (aref (third left) position)
                                      (aref (third right) position))
        unless (zerop comparison)
          return (if (cdr entry) (> comparison 0) (< comparison 0))
        finally (return (< (second left) (second right)))))

(defun write-query-sort-run (items order)
  (with-query-temp-output (pathname output)
    (dolist (item
             (stable-sort items
                          (lambda (left right)
                            (select-sort-item-less-p left right order))))
      (query-spill-write-record output item))
    pathname))

(defun merge-query-sort-runs (left-path right-path order)
  "Merge two runs into an owned output before deleting the inputs."
  (let ((output-path nil))
    (with-query-temp-output (pathname output)
      (setf output-path pathname)
      (progn
        (with-query-temp-input (left-input left-path)
          (with-query-temp-input (right-input right-path)
            (let ((left (query-spill-read-record left-input))
                  (right (query-spill-read-record right-input)))
              (loop until (or (eq left +query-spill-eof+)
                              (eq right +query-spill-eof+))
                    do (if (select-sort-item-less-p left right order)
                           (progn
                             (query-spill-write-record output left)
                             (setf left
                                   (query-spill-read-record left-input)))
                           (progn
                             (query-spill-write-record output right)
                             (setf right
                                   (query-spill-read-record right-input)))))
              (loop until (eq left +query-spill-eof+)
                    do (query-spill-write-record output left)
                       (setf left
                             (query-spill-read-record left-input)))
              (loop until (eq right +query-spill-eof+)
                    do (query-spill-write-record output right)
                       (setf right
                             (query-spill-read-record right-input))))))
        output-path))
    (delete-query-temp-file left-path)
    (delete-query-temp-file right-path)
    output-path))

(defun merge-query-sort-run-pass (runs order)
  (loop for tail on runs by #'cddr
        collect (if (cdr tail)
                    (merge-query-sort-runs (car tail) (cadr tail) order)
                    (car tail))))

(defun stream-select-sort (streamer order callback &key on-complete)
  "Sort a stream with bounded in-memory runs and stable external merges."
  (call-with-query-sort-arena
   (lambda (arena)
     (stream-select-sort-with-arena streamer order callback arena on-complete))))

(defun datum-origins-for-spill (origins)
  "Represent detached origins with ordinary vectors and lists for sort/spill."
  (when origins
    (map 'vector
         (lambda (origin)
           (when origin
             (list (datum-origin-external-p origin)
                   (heap-compressed-varlena-payload (datum-origin-compressed origin)))))
         origins)))

(defun datum-origins-from-spill (origins)
  "Restore detached origins after a sorted row has been copied or spilled."
  (when origins
    (map 'vector
         (lambda (origin)
           (when origin
             (make-datum-origin :external-p (first origin)
                                :compressed (make-heap-compressed-varlena :payload (copy-seq (second origin))))))
         origins)))

(defun stream-select-sort-with-arena (streamer order callback arena &optional on-complete)
  "Sort streamed rows in memory or owned temporary runs and pass them to CALLBACK."
  (with-query-temp-files ()
                         (let ((buffer '())
                               (buffer-bytes 0)
                               (sequence 0)
                               (runs '())
                               (memory-limit (max 1 *query-work-memory-bytes*)))
                           (labels ((own-run ()
                                      ;; Batch region switches: SBCL discards unused TLAB space on
                                      ;; every exit. Staging and the arena copy briefly coexist.
                                      (when arena
                                        (setf buffer
                                              (call-in-query-sort-arena
                                               arena (lambda () (copy-query-sort-value buffer))))))
                                    (flush-run ()
                                      (when buffer
                                        (own-run)
                                        (push (write-query-sort-run (nreverse buffer) order) runs)
                                        (setf buffer nil buffer-bytes 0)
                                        (reset-query-sort-arena arena)))
                                    (make-item (row)
                                      (let ((sequence-number sequence))
                                        (incf sequence)
                                        (list :query-sort-item sequence-number
                                              (map 'vector
                                                   (lambda (entry) (select-order-value (car entry) row))
                                                   order)
                                              row
                                              (and *datum-origin-capture*
                                                   (datum-origins-for-spill *query-row-origins*)))))
                                    (emit-run (pathname)
                                      (with-query-temp-input (stream pathname)
                                        (loop for item = (query-spill-read-record stream)
                                              until (eq item +query-spill-eof+)
                                              do (check-for-interrupts)
                                                 (let ((*query-row-origins* (and *datum-origin-capture*
                                                                                 (datum-origins-from-spill (fifth item)))))
                                                   (when (eq (funcall callback (fourth item)) :stop)
                                                     (return-from stream-select-sort-with-arena :stop)))))))
                             (unwind-protect
                                  (progn
                                    (funcall
                                     streamer
                                     (lambda (row)
                                       (check-for-interrupts)
                                       (let* ((item (make-item row))
                                              (item-bytes (query-spill-record-size item)))
                                         (when (and buffer
                                                    (> (+ buffer-bytes item-bytes) memory-limit))
                                           (flush-run))
                                         (push item buffer)
                                         (incf buffer-bytes item-bytes)
                                         (when (>= buffer-bytes memory-limit)
                                           (flush-run)))))
                                    (if (null runs)
                                        (progn
                                          (own-run)
                                          (dolist (item
                                                   (stable-sort
                                                    (nreverse buffer)
                                                    (lambda (left right)
                                                      (select-sort-item-less-p left right order))))
                                            (check-for-interrupts)
                                            ;; Consumers, including instrumentation, can retain rows.
                                            (let ((*query-row-origins* (and *datum-origin-capture*
                                                                            (datum-origins-from-spill (fifth item)))))
                                              (when (eq (funcall callback
                                                                 (if arena
                                                                     (copy-query-sort-value (fourth item))
                                                                     (fourth item)))
                                                        :stop)
                                                (return-from stream-select-sort-with-arena :stop)))))
                                        (progn
                                          (flush-run)
                                          (setf runs (nreverse runs))
                                          (loop while (cdr runs)
                                                do (setf runs (merge-query-sort-run-pass runs order)))
                                          (emit-run (first runs))))
                                    (when on-complete (funcall on-complete)))
                               ;; This captured cell lives on the heap. Clear it before arena reset
                               ;; or destruction, including cancellation and downstream errors.
                               (setf buffer nil))))))

(defparameter +hash-aggregate-partition-count+ 16)
(defparameter +hash-aggregate-entry-overhead-bytes+ 128)
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

(defun hash-aggregate-sort-spill (pathname group-keys aggregate-count callback)
  "Sort one non-shrinking spill batch and reduce adjacent equal groups."
  (let ((order (mapcar (lambda (expr) (cons expr nil)) group-keys))
        (current-key nil) (current-state nil) (current-origins nil) (saw-row nil))
    (labels ((emit-current ()
               (when current-state
                 (let ((*query-row-origins*
                         (and *datum-origin-capture*
                              (concatenate 'vector (or current-origins #())
                                           (make-array aggregate-count :initial-element nil)))))
                   (prog1 (funcall callback (hash-aggregate-state-row current-state))
                     (setf current-state nil current-origins nil)))))
             (consume (environment)
               (multiple-value-bind (values key origins)
                   (hash-aggregate-group group-keys (cdr environment))
                 (when (and saw-row (not (equal key current-key)))
                   (when (eq (emit-current) :stop) (return-from consume :stop)))
                 (unless current-state
                   (setf current-state (cons (coerce values 'vector)
                                             (make-array aggregate-count :initial-element 0))
                         current-origins origins))
                 (setf saw-row t current-key key)
                 (dotimes (index aggregate-count) (incf (aref (cdr current-state) index)))))
             (source (row-callback)
               (hash-aggregate-spill-file
                pathname (lambda (row) (funcall row-callback (cons nil row))))))
      (let ((result (stream-select-sort #'source order #'consume)))
        (when (eq result :stop)
          (return-from hash-aggregate-sort-spill :stop)))
      (when (and (not saw-row) (null group-keys))
        (setf current-state (cons #() (make-array aggregate-count :initial-element 0))))
      (emit-current))))

(defun hash-aggregate-process-rows
    (source group-keys aggregate-count callback memory-limit depth)
  "Aggregate a row stream, spilling unseen groups and recursively partitioning them.

The spill stream closes normally after the source returns and is abort-cleaned
on an abnormal exit."
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
                              (+ +hash-aggregate-entry-overhead-bytes+
                                 (query-spill-record-size (list key values (datum-origins-for-spill origins))))))
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
      (dolist (state (nreverse group-order))
        (when (eq (let ((*query-row-origins*
                          (and *datum-origin-capture*
                               (concatenate 'vector (or (gethash state state-origins) #())
                                            (make-array aggregate-count :initial-element nil)))))
                    (funcall callback (hash-aggregate-state-row state))) :stop)
          (return-from hash-aggregate-process-rows :stop)))
      (when spill-path
        ;; Drop this batch's keys and states before processing child batches.
        ;; PostgreSQL resets its hash table between batches for the same reason.
        (setf groups (make-hash-table :test #'equal)
              group-order nil
              state-origins (make-hash-table :test #'eq)
              memory-used 0)
        (when (and (> spill-count 0)
                   (>= depth +hash-aggregate-max-partition-depth+))
          (unwind-protect
               (return-from hash-aggregate-process-rows
                 (hash-aggregate-sort-spill spill-path group-keys
                                            aggregate-count callback))
            (delete-query-temp-file spill-path)))
        (let ((partitions
                (hash-aggregate-partition-spill spill-path group-keys (1+ depth))))
          (when (and (> spill-count 0)
                     (some (lambda (partition)
                             (>= (car partition) spill-count))
                           partitions))
            (hash-aggregate-delete-partitions partitions)
            (unwind-protect
                 (return-from hash-aggregate-process-rows
                   (hash-aggregate-sort-spill spill-path group-keys
                                              aggregate-count callback))
              (delete-query-temp-file spill-path)))
          (delete-query-temp-file spill-path)
          (dolist (partition partitions)
            (let* ((path (cdr partition))
                   (result
                     (hash-aggregate-process-rows
                      (lambda (row-callback)
                        (hash-aggregate-spill-file path row-callback))
                      group-keys aggregate-count callback memory-limit
                      (1+ depth))))
              (delete-query-temp-file path)
              (when (eq result :stop)
                (return-from hash-aggregate-process-rows :stop))))))
      (values))))

(defun stream-hash-aggregate (source group-keys aggregate-count callback)
  "Run bounded hash aggregation with partitioned spill and sort fallback."
  (let ((keys (loop for key in group-keys for slot from 1
                    collect (list :col slot :type (expr-type key) :typmod (expr-typmod key)))))
    (with-query-temp-files ()
                           (hash-aggregate-process-rows
                            (lambda (emit)
                              (funcall source
                                       (lambda (environment)
                                         (multiple-value-bind (values key origins)
                                             (hash-aggregate-group group-keys environment)
                                           (declare (ignore key))
                                           (let ((*query-row-origins* origins))
                                             (funcall emit (coerce values 'vector)))))))
                            keys aggregate-count callback (max 1 *query-work-memory-bytes*) 0))))

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

(defun make-aggregate-transition-states (descriptors)
  (let ((budget (max 1 (floor *query-work-memory-bytes* (max 1 (length descriptors))))))
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
                     (:array-agg (make-query-row-spool budget))))
         descriptors)))

(defun close-aggregate-transition-states (states)
  (when states
    (loop for state across states
          when (query-row-spool-p state)
            do (close-query-row-spool state)
               (when (query-row-spool-pathname state)
                 (delete-query-temp-file (query-row-spool-pathname state))))))

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
             (:array-agg (query-row-spool-append state value)))))

(defun finalize-aggregate-transition-states (states)
  (map 'vector
       (lambda (state)
         (if (query-row-spool-p state)
             (if (zerop (query-row-spool-count state)) +null+
                 (let ((result (make-array (query-row-spool-count state))))
                   (query-row-spool-seal state)
                   (map-query-row-spool state (lambda (value index)
                                                (check-for-interrupts)
                                                (setf (aref result index) value)))
                   (make-sql-array :element-type-id :text :elements result
                                   :dimensions (list (length result)) :lower-bounds (list 1))))
             (scalar-aggregate-transition-result state)))
       states))

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

(defun stream-sorted-aggregates (source group-keys descriptors callback)
  "Keep one group's spillable transitions; final output allocation is unavoidable."
  (with-query-temp-files ()
                         (let* ((width (length group-keys))
                                (keys (loop for key in group-keys for position from 1
                                            collect (list :col position :type (expr-type key) :typmod (expr-typmod key))))
                                (states nil) (current-key nil) (current-values nil)
                                (group-origins nil) (transition-origins nil))
                           (labels ((emit-current ()
                                      (when states
                                        (unwind-protect
                                             (let ((*query-row-origins*
                                                     (and *datum-origin-capture*
                                                          (concatenate 'vector group-origins transition-origins))))
                                               (funcall callback
                                                        (concatenate 'vector current-values
                                                                     (finalize-aggregate-transition-states states))))
                                          (close-aggregate-transition-states states)
                                          (setf states nil))))
                                    (consume (row)
                                      (let ((environment (cdr row)))
                                        (multiple-value-bind (values key) (hash-aggregate-group keys environment)
                                          (when (and states (not (equal key current-key)))
                                            (when (eq (emit-current) :stop) (return-from consume :stop)))
                                          (unless states
                                            (setf states (make-aggregate-transition-states descriptors)
                                                  current-key key current-values (coerce values 'vector)
                                                  group-origins (make-array width :initial-element nil)
                                                  transition-origins (make-array (length descriptors) :initial-element nil))
                                            (when (and *datum-origin-capture* *query-row-origins*)
                                              (dotimes (i width)
                                                (setf (aref group-origins i) (aref *query-row-origins* i)))))
                                          (when *datum-origin-capture*
                                            (loop for descriptor in descriptors for i from 0
                                                  for value = (aref environment (+ width i))
                                                  when (and (member (getf descriptor :kind) '(:min :max))
                                                            (not (eq value +null+)))
                                                    do (setf (aref transition-origins i)
                                                             (if (eq (aref states i) +null+)
                                                                 (and *query-row-origins*
                                                                      (aref *query-row-origins* (+ width i)))
                                                                 nil))))
                                          (step-aggregate-transition-states states descriptors environment width))))
                                    (finish ()
                                      (when (and (null states) (null group-keys))
                                        (setf states (make-aggregate-transition-states descriptors)
                                              current-values #() group-origins #()
                                              transition-origins (make-array (length descriptors) :initial-element nil)))
                                      (emit-current))
                                    (input (row-callback)
                                      (funcall source
                                               (lambda (environment)
                                                 (multiple-value-bind (values origins)
                                                     (aggregate-input-row environment group-keys descriptors)
                                                   (let ((*query-row-origins* origins))
                                                     (funcall row-callback (cons nil values))))))))
                             (unwind-protect
                                  (if group-keys
                                      (stream-select-sort #'input (mapcar (lambda (key) (cons key nil)) keys)
                                                          #'consume :on-complete #'finish)
                                      (progn (input #'consume) (finish)))
                               (close-aggregate-transition-states states))))))

(defun stream-select-aggregates (source group-keys specification callback)
  (if (integerp specification)
      (stream-hash-aggregate source group-keys specification callback)
      (stream-sorted-aggregates source group-keys specification callback)))

(defun make-sql-set-function-iterator (call environment)
  (let ((arguments (mapcar (lambda (argument) (eval-expr argument environment)) (third call))))
    (cond ((eq (first call) :single-record)
           (let ((value (eval-func (second call) arguments)) (pending t))
             (lambda ()
               (if pending (progn (setf pending nil) (values value t))
                   (values nil nil)))))
          ((string= (second call) "pg_get_replication_slots")
           (make-replication-slot-result-iterator))
          ((string= (second call) "pg_get_publication_tables")
           (apply #'make-publication-table-result-iterator arguments))
          (t (ereport "0A000" "unsupported set-returning function ~a" (second call))))))

(defun stream-set-iterator-rows (environment iterators callback)
  "Zip independent sets, pad exhausted calls with NULL, and suppress all-empty rows."
  (loop
    (let ((any-row-p nil)
          (values (make-array (length iterators) :initial-element +null+)))
      (declare (type simple-vector values))
      (loop for iterator in iterators for position from 0
            do (check-for-interrupts)
               (multiple-value-bind (value present-p) (funcall iterator)
                 (when present-p
                   (setf (aref values position) value any-row-p t))))
      (unless any-row-p (return))
      (when (eq (funcall callback (concatenate 'vector environment values)) :stop)
        (return :stop)))))

(defun stream-project-set (environment calls callback)
  (stream-set-iterator-rows
   environment
   (mapcar (lambda (call) (make-sql-set-function-iterator call environment)) calls)
   callback))

(defun stream-union-all (arms emit run-child)
  "An arm's LIMIT ends that arm; only a consumer stop ends the Append."
  (let ((stopped-p nil))
    (dolist (arm arms)
      (funcall run-child (second arm)
               (lambda (row)
                 (let ((values (make-array (length (first arm))))
                       (origins (and *datum-origin-capture*
                                     (make-array (length (first arm)) :initial-element nil))))
                   (loop for expression in (first arm) for i from 0
                         do (multiple-value-bind (value origin)
                                (if *datum-origin-capture*
                                    (eval-expr-with-origin expression (car row) *query-row-origins*)
                                    (values (eval-expr expression (car row)) nil))
                              (setf (aref values i) value)
                              (when origins (setf (aref origins i) origin))))
                   (let ((*query-row-origins* origins))
                     (when (eq (funcall emit (cons values values)) :stop)
                       (setf stopped-p t)
                       :stop)))))
      (when stopped-p (return :stop)))))

(defun simple-datum-origin-plan-p (node &optional relations iterator-p worktable)
  "Recognize plans whose execution retains detached row origins."
  (case (first node)
    ((:result :seq-scan :index-scan) t)
    ((:filter :project :sort :limit)
     (simple-datum-origin-plan-p (third node) relations iterator-p worktable))
    (:top-n (simple-datum-origin-plan-p (fourth node) relations iterator-p worktable))
    (:hash-distinct (simple-datum-origin-plan-p (third node) relations iterator-p worktable))
    (:append
     (every (lambda (arm)
              (simple-datum-origin-plan-p (second arm) relations iterator-p worktable))
            (second node)))
    ((:sorted-aggregate :hash-aggregate)
     (simple-datum-origin-plan-p (fourth node) relations iterator-p worktable))
    (:subquery-scan (simple-datum-origin-plan-p (fourth node) relations iterator-p worktable))
    (:nested-loop-join
     (and (simple-datum-origin-plan-p (third node) relations iterator-p worktable)
          (simple-datum-origin-plan-p (fourth node) relations iterator-p worktable)))
    (:with-ctes
        (and (not iterator-p)
             (simple-datum-origin-plan-p (third node) relations)))
    (:cte-scan
     (and (not iterator-p) relations
          (let* ((relation (physical-scan-relation node relations))
                 (definition (getf relation :cte))
                 (plan (or (getf relation :cte-reference-plan)
                           (and definition (sql-cte-plan definition)))))
            (and plan
                 (simple-datum-origin-plan-p (query-plan-physical plan)
                                             (subquery-registered-relations plan) t)))))
    (:recursive-union
     (and iterator-p
          (simple-datum-origin-plan-p (third node) relations t (second node))
          (simple-datum-origin-plan-p (fourth node) relations t (second node))))
    (:worktable-scan
     (and iterator-p worktable relations
          (eq (getf (physical-scan-relation node relations) :cte) worktable)))
    (otherwise nil)))

(defun execute-physical-plan-node (node registered-relations outer-environment
                                   emit run-child run-node)
  "Execute one physical node using the instrumented callbacks from RUN-NODE."
  (case (first node)
    (:result
     (let ((*query-row-origins* nil)) (funcall emit #())))
    (:with-ctes
        (call-with-cte-definitions (second node) (lambda () (funcall run-child (third node) emit))))
    (:cte-scan
     (let ((reader (make-cte-scan-iterator (physical-scan-relation node registered-relations))))
       (unwind-protect (map-sql-iterator reader emit) (close-sql-iterator reader))))
    (:worktable-scan
     (let ((reader (make-cte-worktable-iterator (physical-scan-relation node registered-relations))))
       (unwind-protect (map-sql-iterator reader emit) (close-sql-iterator reader))))
    (:recursive-union
     (let ((input (make-recursive-union-iterator node registered-relations)))
       (unwind-protect (map-sql-iterator input emit) (close-sql-iterator input))))
    (:append (stream-union-all (second node) emit run-child))
    ((:seq-scan :index-scan)
     (let* ((relation (physical-scan-relation node registered-relations))
            (path (physical-scan-path node relation)))
       (scan-select-relation
        relation path *current-snapshot*
        (lambda (row)
          (check-for-interrupts)
          (let* ((environment (if (getf relation :row-source)
                                  row
                                  (htup-values row)))
                 (*query-row-origins*
                   (and *datum-origin-capture* (not (getf relation :row-source))
                        (if (getf relation :join-order-base)
                            (make-select-relation-origins registered-relations relation (htup-origins row))
                            (htup-origins row)))))
            (funcall emit
                     (if (getf relation :join-order-base)
                         (make-select-relation-environment
                          registered-relations relation environment)
                         environment))))
        outer-environment)))
    (:subquery-scan
     (physical-scan-relation node registered-relations)
     (let* ((parent-frames *subquery-outer-frames*)
            (frame (cons (if (getf (physical-scan-relation node registered-relations) :lateral-dependent)
                             outer-environment #())
                         (make-hash-table :test #'eq)))
            (*subquery-outer-frames* (cons frame parent-frames)))
       (when (and *datum-origin-capture*
                  (getf (physical-scan-relation node registered-relations) :lateral-dependent))
         (setf (gethash +subquery-frame-origins-key+ (cdr frame))
               (datum-origins-from-spill (datum-origins-for-spill *query-row-origins*))))
       (unwind-protect
            (funcall run-child (fourth node)
                     (lambda (row)
                       (let ((*subquery-outer-frames* parent-frames))
                         (funcall emit (car row)))))
         (close-cte-frame-cache (cdr frame)))))
    (:filter
     (let ((qual (second node)))
       (funcall run-child
                (third node)
                (lambda (environment)
                  (when (true-p (eval-expr qual environment))
                    (funcall emit environment))))))
    (:nested-loop-join
     (let* ((join (second node))
            (left (third node))
            (right (fourth node))
            (right-relation
              (and (member (first right) '(:seq-scan :index-scan) :test #'eq)
                   (physical-scan-relation right registered-relations)))
            (right-path
              (and right-relation
                   (physical-scan-path right right-relation))))
       (if (or (getf join :lateral-dependent)
               (and right-path (select-scan-path-parameter-expr right-path)))
           (stream-select-indexed-join-environments
            (lambda (left-callback)
              (funcall run-child left left-callback))
            (lambda (outer-environment right-callback)
              (funcall run-node right right-callback outer-environment))
            join emit)
           (stream-select-join-environments
            (lambda (left-callback)
              (funcall run-child left left-callback))
            (lambda (right-callback)
              (funcall run-child right right-callback))
            join emit))))
    (:window-input
     (funcall run-child (third node)
              (lambda (environment)
                (funcall emit
                         (concatenate 'vector environment
                                      (map 'vector (lambda (expression) (eval-expr expression environment))
                                           (second node)))))))
    (:window
     (stream-window-stage (lambda (callback) (funcall run-child (third node) callback))
                          (second node) emit))
    (:project-set
     (funcall run-child (third node)
              (lambda (environment) (stream-project-set environment (second node) emit))))
    (:project
     (let ((targets (second node)))
       (funcall run-child
                (third node)
                (lambda (environment)
                  (if *datum-origin-capture*
                      (let ((values (make-array (length targets)))
                            (origins (make-array (length targets) :initial-element nil)))
                        (loop for target in targets for i from 0
                              do (multiple-value-bind (value origin)
                                     (eval-expr-with-origin (getf target :expr) environment *query-row-origins*)
                                   (setf (aref values i) value (aref origins i) origin)))
                        (let ((*query-row-origins* origins))
                          (funcall emit (cons values environment))))
                      (let ((row (make-select-result-row environment nil targets)))
                        (when row (funcall emit row))))))))
    (:hash-distinct
     (stream-hash-aggregate
      (lambda (callback)
        (funcall run-child (third node)
                 (lambda (row) (funcall callback (car row)))))
      (second node) 0
      (lambda (values) (funcall emit (cons values values)))))
    (:sort
     (stream-select-sort
      (lambda (sort-callback)
        (funcall run-child (third node) sort-callback))
      (second node) emit))
    (:top-n
     (let* ((order (second node))
            (limit (third node))
            (input (fourth node))
            (rows (collect-select-top-n
                   (lambda (callback)
                     (funcall run-child input callback))
                   order limit *datum-origin-capture*)))
       (loop for row in rows
             do (let ((*query-row-origins* (and *datum-origin-capture* (cdr row))))
                  (when (eq (funcall emit (if *datum-origin-capture* (car row) row)) :stop)
                    (return :stop))))))
    (:limit
     (let ((limit (second node))
           (count 0))
       (unless (zerop limit)
         (funcall run-child
                  (third node)
                  (lambda (row)
                    (incf count)
                    (when (or (eq (funcall emit row) :stop)
                              (>= count limit))
                      :stop))))))
    ((:hash-aggregate :sorted-aggregate)
     (stream-select-aggregates
      (lambda (aggregate-callback)
        (funcall run-child (fourth node) aggregate-callback))
      (second node) (third node) emit))
    (otherwise
     (error "Unknown physical query-plan node ~s" node))))

(defun reset-query-plan-instrumentation (metadata-map)
  (maphash (lambda (node metadata)
             (declare (ignore node))
             (setf (plan-node-metadata-actual-rows metadata) nil
                   (plan-node-metadata-actual-row-count metadata) 0
                   (plan-node-metadata-actual-loops metadata) 0
                   (plan-node-metadata-actual-startup-time-ticks metadata) 0
                   (plan-node-metadata-actual-total-time-ticks metadata) 0))
           metadata-map)
  (values))

(defun record-query-plan-node-instrumentation (metadata started emitted startup-time
                                               callback-time child-total-time)
  (when (and started metadata)
    (incf (plan-node-metadata-actual-loops metadata))
    (incf (plan-node-metadata-actual-row-count metadata) emitted)
    (setf (plan-node-metadata-actual-rows metadata)
          (/ (plan-node-metadata-actual-row-count metadata)
             (plan-node-metadata-actual-loops metadata)))
    (let* ((elapsed (- (get-internal-real-time) started))
           (self-time (max 0 (- elapsed callback-time child-total-time))))
      (incf (plan-node-metadata-actual-startup-time-ticks metadata)
            (or startup-time elapsed))
      (incf (plan-node-metadata-actual-total-time-ticks metadata)
            (+ self-time child-total-time))))
  (values))

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
                   (or (eq (first scan) :seq-scan) (null (seventh scan))))
          (let* ((relation (physical-scan-relation scan relations))
                 (table (getf relation :table))
                 (slots nil))
            (labels ((walk (expr)
                       (and (consp expr)
                            (case (first expr)
                              ((:const :bool) t)
                              (:col
                               (let ((slot (second expr)))
                                 (when (and (integerp slot)
                                            (<= 1 slot (length (pg-class-columns table))))
                                   (pushnew slot slots)
                                   t)))
                              ((:cast :not :is-null :is-bool) (walk (second expr)))
                              (:unary (walk (third expr)))
                              ((:and :or :concat)
                               (and (walk (second expr)) (walk (third expr))))
                              (:binop (and (walk (third expr)) (walk (fourth expr))))
                              (:func (and (listp (third expr)) (every #'walk (third expr))))
                              (otherwise nil))))
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
                    (and (= (length args) 1) (eq input column)
                         (member (expr-type column) '(:text :varchar :bytea))
                         (member (second expression) '("length" "octet_length" "char_length" "character_length")
                                 :test #'string=)
                         (or (not (eq (expr-type column) :bytea))
                             (member (second expression) '("length" "octet_length") :test #'string=)))))
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

(defun interpret-query-plan (plan registered-relations row-callback)
  "Reference interpreter for physical S-expression plans."
  (let* ((metadata-map (query-plan-metadata plan))
         (*select-toast-slice-plan* (simple-select-toast-slice (query-plan-physical plan) registered-relations))
         (*toast-slice-current* nil)
         (*select-toast-demand* (simple-select-toast-demand (query-plan-physical plan) registered-relations))
         (*subquery-instrumentation-depth* (1+ *subquery-instrumentation-depth*)))
    (when (and *select-toast-slice-plan* *select-toast-demand*)
      (setf *select-toast-demand*
            (cons (car *select-toast-demand*)
                  (set-difference (cdr *select-toast-demand*)
                                  (mapcar #'cdr (cdr *select-toast-slice-plan*))))))
    (when (and *query-plan-instrumentation-p* (= *subquery-instrumentation-depth* 1))
      (reset-query-plan-tree-instrumentation plan))
    (labels ((run-node (node callback &optional outer-environment)
               (let ((metadata (gethash node metadata-map))
                     (emitted 0)
                     (started (when *query-plan-instrumentation-p*
                                (get-internal-real-time)))
                     (startup-time nil)
                     (callback-time 0)
                     (child-total-time 0))
                 (labels ((emit (row)
                            (incf emitted)
                            (when (and started (null startup-time))
                              (setf startup-time
                                    (- (get-internal-real-time) started)))
                            (if started
                                (let ((callback-start (get-internal-real-time)))
                                  (unwind-protect
                                       (funcall callback row)
                                    (incf callback-time
                                          (- (get-internal-real-time)
                                             callback-start))))
                                (funcall callback row)))
                          (run-child (child child-callback &optional environment)
                            (let* ((child-metadata (gethash child metadata-map))
                                   (old-total-time
                                     (if child-metadata
                                         (plan-node-metadata-actual-total-time-ticks
                                          child-metadata)
                                         0)))
                              (unwind-protect
                                   (run-node child child-callback environment)
                                (when (and started child-metadata)
                                  (incf child-total-time
                                        (- (plan-node-metadata-actual-total-time-ticks
                                            child-metadata)
                                           old-total-time)))))))
                   (unwind-protect
                        (execute-physical-plan-node
                         node registered-relations outer-environment
                         #'emit #'run-child #'run-node)
                     (record-query-plan-node-instrumentation
                      metadata started emitted startup-time callback-time
                      child-total-time))))))
      (run-node (query-plan-physical plan)
                (lambda (row)
                  (let ((*select-toast-demand* nil)
                        (*select-toast-slice-plan* nil)
                        (*toast-slice-current* nil))
                    (funcall row-callback row)))))))

(register-query-plan-backend :interpreter :executor #'interpret-query-plan)

(defun physical-plan-node-children (node)
  (case (first node)
    (:append (mapcar #'second (second node)))
    ((:with-ctes :window-input :window :filter :project :sort :limit :hash-distinct :project-set) (list (third node)))
    ((:top-n :subquery-scan) (list (fourth node)))
    ((:nested-loop-join :recursive-union) (list (third node) (fourth node)))
    ((:hash-aggregate :sorted-aggregate) (list (fourth node)))
    (otherwise nil)))

(defun reconcile-query-plan-access-paths (plan registered-relations)
  "Keep the physical tree and metadata in sync with registered read paths."
  (labels ((visit (node)
             (when (consp node)
               (when (eq (first node) :index-scan)
                 (let* ((relation (physical-scan-relation
                                   node registered-relations))
                        (path (getf relation :path))
                        (table (getf relation :table))
                        (index (and (eq (select-scan-path-kind path) :index-scan)
                                    (select-scan-path-index path))))
                   (unless (and index
                                (= (pg-index-oid index) (fourth node))
                                (= (pg-index-table-oid index)
                                   (pg-class-oid table)))
                     ;; Revalidation found that DDL replaced the planned index.
                     (setf (first node) :seq-scan
                           (cdddr node) nil)
                     (let ((metadata (gethash node
                                              (query-plan-metadata plan))))
                       (when metadata
                         (setf (plan-node-metadata-selection-rule metadata)
                               :sequential-fallback))))))
               (dolist (child (physical-plan-node-children node))
                 (visit child)))))
    (visit (query-plan-physical plan)))
  plan)

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
                 (when analyze-p
                   (let* ((metadata (gethash node
                                             (query-plan-metadata plan)))
                          (loops (and metadata
                                      (plan-node-metadata-actual-loops metadata))))
                     (if (and loops (plusp loops))
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
                                 loops)
                         (write-string " (never executed)" stream))))
                 (dolist (child (physical-plan-node-children node))
                   (terpri stream)
                   (print-node child (1+ depth)))))
        (print-node (query-plan-physical plan) 0)
        (let ((*subquery-registered-relations* (or *subquery-registered-relations* relations)))
          (loop for descriptor in (query-plan-expression-subplans plan) for number from 1
                for child = (sql-subplan-plan descriptor)
                for child-relations = (subquery-registered-relations child)
                do (reconcile-query-plan-access-paths child child-relations)
                   (format stream "~%  ~a ~d (~a)"
                           (if (and (member (sql-subplan-kind descriptor) '(:scalar :exists :array))
                                    (not (member 1 (sql-subplan-depths descriptor))))
                               "InitPlan" "SubPlan")
                           number (string-downcase (symbol-name (sql-subplan-kind descriptor))))
                   (dolist (line (uiop:split-string
                                  (format-query-plan child child-relations analyze-p nil t)
                                  :separator (string #\Newline)))
                     (format stream "~%    ~a" line))))
        (let ((*subquery-registered-relations* (or *subquery-registered-relations* relations)))
          (dolist (definition (remove-if-not #'sql-cte-shared-p (query-plan-cte-definitions plan)))
            (format stream "~%  CTE ~a (~a)" (sql-cte-name definition)
                    (if (sql-cte-shared-p definition) "shared" "per reference"))
            (dolist (line (uiop:split-string
                           (format-query-plan (sql-cte-plan definition)
                                              (subquery-registered-relations (sql-cte-plan definition))
                                              analyze-p nil t)
                           :separator (string #\Newline)))
              (format stream "~%    ~a" line))))
        (let ((*subquery-registered-relations* (or *subquery-registered-relations* relations)))
          (dolist (node (collect-query-plan-nodes (query-plan-physical plan)))
            (when (eq (first node) :cte-scan)
              (let* ((relation (physical-scan-relation node relations))
                     (reference (getf relation :cte-reference-plan)))
                (when reference
                  (format stream "~%  CTE ~a (per reference)" (sql-cte-name (getf relation :cte)))
                  (dolist (line (uiop:split-string
                                 (format-query-plan reference (subquery-registered-relations reference)
                                                    analyze-p nil t)
                                 :separator (string #\Newline)))
                    (format stream "~%    ~a" line)))))))
        (when (and analyze-p (not nested-p))
          (terpri stream)
          (format stream "Execution Time: ~,3f ms" (or execution-time 0.0d0)))))))

(defun select-result-column-signature (column)
  (list (result-column-name column) (result-column-table-oid column)
        (result-column-attnum column) (result-column-type-oid column)
        (result-column-typlen column) (result-column-typmod column)))

(defun exec-select (stream ast)
  (let* ((plan (plan-select-query ast))
         (prepared (query-plan-prepared plan))
         (relations (getf prepared :relations))
         (targets (getf prepared :targets))
         (columns (mapcar #'target->result-column targets))
         (row-count 0))
    (when (and *select-suppress-description*
               (not (equal (mapcar #'select-result-column-signature columns)
                           (mapcar #'select-result-column-signature *select-expected-result-columns*))))
      (error "SELECT result metadata changed after Bind"))
    (labels ((send-row (row)
               (send-select-row stream row targets)
               (incf row-count)))
      (if relations
          (with-registered-select-relations (registered relations)
            (reconcile-query-plan-access-paths plan registered)
            (unless *select-suppress-description* (send-row-description stream columns))
            (execute-query-plan plan registered #'send-row))
          (progn
            (unless *select-suppress-description* (send-row-description stream columns))
            (execute-query-plan plan nil #'send-row)))
      (send-command-complete stream (format nil "SELECT ~a" row-count)))))

(defun copy-write-frame (stream type payload)
  "A partially transmitted COPY frame must close rather than interleave an error."
  (let ((complete nil))
    (unwind-protect
         (prog1 (write-message stream type payload) (setf complete t))
      (unless complete (ignore-errors (close stream :abort t))))))

(defun copy-text-field (text output)
  (if (null text)
      (write-string "\\N" output)
      (loop for character across text
            for code = (char-code character)
            for escaped = (case code
                            (8 #\b) (9 #\t) (10 #\n) (11 #\v) (12 #\f) (13 #\r) (92 #\\))
            do (when (zerop code) (ereport "22021" "COPY text cannot contain a zero byte"))
               (when escaped (write-char #\\ output))
               (write-char (or escaped character) output))))

(defun copy-text-row (values targets)
  (string->bytes
   (with-output-to-string (output)
     (loop for value across values for target in targets for index from 0
           do (when (plusp index) (write-char #\Tab output))
              (copy-text-field (value->text value (getf target :type)) output))
     (write-char #\Newline output))))

(defun send-copy-out-response (stream column-count)
  (let ((payload (make-payload)))
    (payload-push-byte payload 0)
    (payload-push-u16 payload column-count)
    (dotimes (index column-count) (payload-push-u16 payload 0))
    (copy-write-frame stream #\H payload)))

(defun send-copy-command-complete (stream count)
  (let ((payload (make-payload)))
    (payload-push-cstring payload (format nil "COPY ~d" count))
    (copy-write-frame stream #\C payload)))

(defun exec-copy-to (stream ast)
  "Stream a SELECT plan's snapshot rows using C's default text COPY format."
  (let* ((plan (plan-select-query (getf ast :query)))
         (prepared (query-plan-prepared plan))
         (relations (getf prepared :relations))
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
               (execute-query-plan plan registered #'send-row)))
      (if relations
          (with-registered-select-relations (registered relations)
            (reconcile-query-plan-access-paths plan registered)
            (copy-plan registered))
          (copy-plan nil))
      (copy-write-frame stream #\c #())
      count)))

(defun exec-explain (stream ast)
  (let* ((plan (plan-select-query (getf ast :statement)))
         (prepared (query-plan-prepared plan))
         (relations (getf prepared :relations))
         (analyze-p (getf ast :analyze-p))
         (description nil)
         (execution-time 0.0d0))
    (labels ((prepare-explain (registered)
               (reconcile-query-plan-access-paths plan registered)
               (when analyze-p
                 (let ((started (get-internal-real-time)))
                   (let ((*query-plan-instrumentation-p* t))
                     (execute-query-plan
                      plan registered
                      (lambda (row)
                        (declare (ignore row))
                        nil)))
                   (setf execution-time
                         (internal-time-ticks->milliseconds
                          (- (get-internal-real-time) started)))))
               (setf description
                     (format-query-plan plan registered analyze-p
                                        execution-time))))
      (if relations
          (with-registered-select-relations (registered relations)
            (prepare-explain registered))
          (prepare-explain nil)))
    (send-row-description
     stream
     (list (make-result-column :name "QUERY PLAN"
                               :type-oid +textoid+
                               :typlen -1
                               :typmod -1)))
    (send-data-row-text stream (list description))
    (send-command-complete stream "EXPLAIN")))
