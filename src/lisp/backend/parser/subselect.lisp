;;;; subselect.lisp -- expression subqueries, query boundaries and parameters.
(in-package #:pg)

(defvar *select-outer-scope* nil)

(defvar *select-analysis-frame* nil)

(defvar *defer-group-rewrite-p* nil)

(defvar *select-sublinks* nil)

(defvar *select-sublinks-allowed-p* nil)

(defvar *subquery-execution-active-p* nil)

(defvar *subquery-result-cache* nil)

(defvar *subquery-registered-relations* nil)

(defvar *subquery-outer-frames* nil)

(defvar *subquery-output-value* nil)


(defvar *subquery-instrumentation-depth* 0)

(defun subquery-default-name (expression)
  (cond ((eq (first expression) :cast) (subquery-default-name (second expression)))
        ((subquery-expression-p expression)
         (let ((name (getf (cddr expression) :result-name)))
           (unless (equal name "?column?") name)))))

(defstruct sql-expression-key
  ;; Immutable signature data is opaque to walkers, not an executable expression.
  value)

(defstruct sql-subplan
  ;; DEPTHS describes outer-frame dependencies. GROUPED-CONTEXTS records scopes
  ;; already rewritten to group output slots, avoiding a second slot rewrite.
  kind prepared plan test depths grouped-contexts)

(defun local-expression-operand-shape (tag)
  "Identify the executable operand layout for an analyzed expression tag."
  (case tag
    ((:const :bool :scalar-array-const :col :sort-output :window-ref) :leaf)
    ((:cast :not :is-null :is-bool :join-relabel :record-field :array :coalesce) '(1))
    ((:func :set-func :single-record :whole-row :unary) '(2))
    ((:and :or :concat :join-coalesce :array-length :array-subscript :array-slice) '(1 2))
    ((:binop :any :all) '(2 3))
    ((:array-assign :array-slice-assign) '(1 2 3))
    (:outer-ref :outer-ref)
    (:subplan :subplan)
    (:in :in)
    (:case :case)
    (otherwise :structural)))

(defun some-analyzed-in-operand (predicate form)
  "Visit scalar and typed IN operands, skipping the surrounding option plist."
  (let ((options (cddddr form)))
    (or (funcall predicate (second form))
        (some predicate (third form))
        (let ((left (getf options :array-left)))
          (and left (funcall predicate left)))
        (some predicate (getf options :array-items))
        (some (lambda (pair) (some predicate pair)) (getf options :comparisons)))))

(defun some-analyzed-case-operand (predicate form)
  "Visit the WHEN conditions, THEN values, and ELSE value."
  (or (some (lambda (branch)
              (or (funcall predicate (first branch))
                  (funcall predicate (second branch))))
            (second form))
      (funcall predicate (third form))))

(defun some-local-plan-expression (predicate expression)
  "Visit local expression nodes until PREDICATE succeeds.
PREDICATE receives the node and may call the walker before returning.
Its second value can suppress descent without stopping the walk.
Subplans expose their test without entering the prepared query. Outer
references expose the operand evaluated in the outer frame."
  (labels ((walk (form)
             (when (consp form)
               (multiple-value-bind (found skip-children-p)
                   (funcall predicate form #'walk)
                 (or found
                     (unless skip-children-p
                       (let ((shape (local-expression-operand-shape (first form))))
                         (case shape
                           (:leaf nil)
                           (:in (some-analyzed-in-operand #'walk form))
                           (:case (some-analyzed-case-operand #'walk form))
                           (:subplan
                            (let ((subplan (second form)))
                              (and (sql-subplan-p subplan) (walk (sql-subplan-test subplan)))))
                           (:outer-ref (walk (third form)))
                           (:structural (or (walk (car form)) (walk (cdr form))))
                           (otherwise (some (lambda (position) (walk (nth position form))) shape))))))))))
    (declare (dynamic-extent #'walk))
    (not (null (walk expression)))))

(defun subquery-expression-p (expression)
  (and (consp expression) (eq (first expression) :subplan)))

(defun map-local-expression-operands (function form)
  "Copy FORM's local expression operands. Callers handle outer refs and subplans."
  (let ((shape (local-expression-operand-shape (first form))))
    (case shape
      ((:leaf :outer-ref :subplan) (copy-list form))
      (:in (map-analyzed-in-operands function form))
      (:case
          (let ((copy (copy-list form)))
            (setf (second copy)
                  (mapcar (lambda (branch)
                            (list* (funcall function (first branch))
                                   (funcall function (second branch))
                                   (cddr branch)))
                          (second form))
                  (third copy) (funcall function (third form)))
            copy))
      (:structural (mapcar function form))
      (otherwise
       (let ((copy (copy-list form)))
         (dolist (position shape copy)
           (setf (nth position copy) (funcall function (nth position form)))))))))

(defun claim-query-rewrite (node seen)
  "Claim an owned mutable query node once for this rewrite."
  (unless (gethash node seen)
    (setf (gethash node seen) t)))

(defun map-prepared-query-expressions (function prepared &optional (depth 0) (seen (make-hash-table :test #'eq)))
  "Rewrite executable expressions with nesting depth, excluding saved raw ASTs."
  (labels
      ((expression (form level)
         (cond
           ((atom form) form)
           ((eq (first form) :outer-ref) (funcall function form level))
           ((subquery-expression-p form)
            (let ((descriptor (second form)))
              (when (claim-query-rewrite descriptor seen)
                (setf (sql-subplan-test descriptor)
                      (expression (sql-subplan-test descriptor) level))
                (query (sql-subplan-prepared descriptor) (1+ level))))
            (funcall function form level))
           (t (funcall function
                       (map-local-expression-operands
                        (lambda (part) (expression part level)) form) level))))
       (query (state level)
         (unless (and state (claim-query-rewrite state seen))
           (return-from query nil))
         (dolist (definition (getf state :ctes))
           (query (sql-cte-prepared definition) (1+ level)))
         (if (getf state :set-operation)
             (progn
               (query (getf state :left) level)
               (query (getf state :right) level))
             (progn
               (setf (getf state :where) (expression (getf state :where) level)
                     (getf state :having) (expression (getf state :having) level)
                     (getf state :group-keys)
                     (mapcar (lambda (form) (expression form level)) (getf state :group-keys))
                     (getf state :set-functions)
                     (mapcar (lambda (form) (expression form level)) (getf state :set-functions)))
               (dolist (target (getf state :targets))
                 (setf (getf target :expr) (expression (getf target :expr) level)))
               (dolist (join (getf state :joins))
                 (setf (getf join :qual) (expression (getf join :qual) level)))
               (setf (getf state :window-keys)
                     (mapcar (lambda (form) (expression form level)) (getf state :window-keys)))
               (dolist (window (getf state :windows))
                 (map-window-expressions (lambda (form) (expression form level)) window))
               (dolist (aggregate (getf state :aggregates))
                 (setf (getf aggregate :input) (expression (getf aggregate :input) level)))
               (dolist (relation (getf state :source-relations (getf state :relations)))
                 (when (getf relation :subquery-prepared)
                   (query (getf relation :subquery-prepared) (1+ level))))))
         (setf (getf state :order)
               (mapcar (lambda (item) (cons (expression (car item) level) (cdr item)))
                       (getf state :order)))))
    (query prepared depth))
  prepared)

(defun visit-prepared-query-local-expressions (function prepared)
  "Visit expression roots in PREPARED without rewriting any query storage."
  (unless (getf prepared :set-operation)
    (funcall function (getf prepared :where))
    (funcall function (getf prepared :having))
    (dolist (key '(:group-keys :set-functions))
      (mapc function (getf prepared key)))
    (dolist (target (getf prepared :targets)) (funcall function (getf target :expr)))
    (dolist (join (getf prepared :joins)) (funcall function (getf join :qual)))
    (mapc function (getf prepared :window-keys))
    (dolist (window (getf prepared :windows))
      (funcall function (getf window :input))
      (mapc function (getf window :partition))
      (dolist (item (getf window :order)) (funcall function (car item))))
    (dolist (aggregate (getf prepared :aggregates)) (funcall function (getf aggregate :input))))
  (dolist (item (getf prepared :order)) (funcall function (car item)))
  (values))

(defun visit-prepared-query-expressions (function prepared &optional (depth 0) query-function descend-p expression-leaf-p)
  "Visit executable nodes and query scopes, pruning scopes rejected by DESCEND-P.
EXPRESSION-LEAF-P can stop descent beneath an already visited node."
  (labels ((expression (form level)
             (some-local-plan-expression
              (lambda (node walk)
                (case (first node)
                  (:outer-ref
                   (funcall function node level)
                   (values nil t))
                  (:subplan
                   (let ((descriptor (second node)))
                     (funcall walk (sql-subplan-test descriptor))
                     (query (sql-subplan-prepared descriptor) (1+ level)))
                   (funcall function node level)
                   (values nil t))
                  (otherwise
                   (funcall function node level)
                   (values nil (and expression-leaf-p (funcall expression-leaf-p node))))))
              form))
           (query (state level)
             (when (and descend-p (not (funcall descend-p state)))
               (return-from query nil))
             (when query-function (funcall query-function state level))
             (dolist (definition (getf state :ctes))
               (query (sql-cte-prepared definition) (1+ level)))
             (when (getf state :set-operation)
               (query (getf state :left) level)
               (query (getf state :right) level))
             (visit-prepared-query-local-expressions
              (lambda (form) (expression form level)) state)
             (unless (getf state :set-operation)
               (dolist (relation (getf state :source-relations (getf state :relations)))
                 (when (getf relation :subquery-prepared)
                   (query (getf relation :subquery-prepared) (1+ level)))))))
    (query prepared depth))
  (values))

(defun prepared-query-outer-depths (prepared)
  ;; Follow referenced producers only: these depths select runtime frame and
  ;; cache ownership, unlike the broader walk used for lateral legality.
  (let ((depths nil) (seen (make-hash-table :test #'eq)))
    (labels
        ((expression (form level)
           (some-local-plan-expression
            (lambda (node walk)
              (case (first node)
                (:outer-ref
                 (when (> (second node) level)
                   (pushnew (- (second node) level) depths))
                 (values nil t))
                (:subplan
                 (let ((subplan (second node)))
                   (funcall walk (sql-subplan-test subplan))
                   (query (sql-subplan-prepared subplan) (1+ level)))
                 (values nil t))))
            form))
         (query (state level)
           (unless (member level (gethash state seen))
             (push level (gethash state seen))
             (if (getf state :set-operation)
                 (progn (query (getf state :left) level) (query (getf state :right) level))
                 (progn
                   (visit-prepared-query-local-expressions
                    (lambda (form) (expression form level)) state)
                   (dolist (relation (getf state :source-relations (getf state :relations)))
                     (cond ((getf relation :cte-worktable) nil)
                           ((getf relation :cte)
                            (query (sql-cte-prepared (getf relation :cte))
                                   (+ level 1 (- (getf relation :cte-levelsup)))))
                           ((getf relation :subquery-prepared)
                            (query (getf relation :subquery-prepared) (1+ level))))))))))
      (query prepared 0))
    (sort depths #'<)))

(defun rewrite-subplan-group-columns (form group-keys scope)
  (let ((descriptor (second form)))
    (when (member scope (sql-subplan-grouped-contexts descriptor) :test #'eq)
      (return-from rewrite-subplan-group-columns form))
    (push scope (sql-subplan-grouped-contexts descriptor))
    (setf (sql-subplan-test descriptor)
          (rewrite-aggregate-columns (sql-subplan-test descriptor) group-keys scope))
    (map-prepared-query-expressions
     (lambda (expression level)
       (if (and (eq (first expression) :outer-ref) (= (second expression) level))
           ;; Raw and coerced IN operands can share the same outer reference.
           (let ((copy (copy-list expression)))
             (setf (third copy)
                   (rewrite-aggregate-columns (third expression) group-keys scope))
             copy)
           expression))
     (sql-subplan-prepared descriptor) 1))
  form)

(defun finalize-query-group-expressions (targets order group-keys scope &optional projected-order-p)
  (let ((rewritten (make-hash-table :test #'eq)))
    (flet ((rewrite (expression)
             (multiple-value-bind (value found) (gethash expression rewritten)
               (if found value
                   (setf (gethash expression rewritten)
                         (rewrite-aggregate-columns expression group-keys scope))))))
      (dolist (target targets)
        (setf (getf target :expr) (rewrite (getf target :expr))))
      (if projected-order-p order
          (mapcar (lambda (item) (cons (rewrite (car item)) (cdr item))) order)))))

(defun aggregate-input-owner-depth (input)
  "Find the nearest query level referenced by the aggregate input."
  (let ((minimum nil))
    (labels ((note (depth)
               (setf minimum (if minimum (min minimum depth) depth)))
             (note-outer (form level)
               (when (and (eq (first form) :outer-ref) (>= (second form) level))
                 (note (- (second form) level)))))
      (some-local-plan-expression
       (lambda (form walk)
         (case (first form)
           (:outer-ref
            (note-outer form 0)
            (values nil t))
           ((:col :whole-row)
            (note 0)
            (values nil t))
           (:subplan
            (let ((descriptor (second form)))
              (funcall walk (sql-subplan-test descriptor))
              (visit-prepared-query-expressions
               #'note-outer
               (sql-subplan-prepared descriptor) 1))
            (values nil t))))
       input))
    (or minimum 0)))

(defun rebase-aggregate-input (input owner-depth)
  (let ((seen nil))
    (labels ((rebase (form level)
               (cond
                 ((atom form) form)
                 ((eq (first form) :outer-ref)
                  (let ((depth (- (second form) owner-depth)))
                    (cond ((and (zerop level) (zerop depth)) (third form))
                          ((>= (second form) (+ level owner-depth))
                           (list :outer-ref depth (third form)
                                 :type (expr-type form) :typmod (expr-typmod form)))
                          (t form))))
                 ((subquery-expression-p form)
                  (let ((descriptor (second form)))
                    (unless seen (setf seen (make-hash-table :test #'eq)))
                    (when (claim-query-rewrite descriptor seen)
                      (setf (sql-subplan-test descriptor)
                            (rebase (sql-subplan-test descriptor) level))
                      (map-prepared-query-expressions
                       (lambda (part nesting)
                         (if (eq (first part) :outer-ref) (rebase part nesting) part))
                       (sql-subplan-prepared descriptor) (1+ level) seen)))
                  form)
                 (t (map-local-expression-operands (lambda (part) (rebase part level)) form)))))
      (rebase input 0))))

(defun subquery-semantic-key (form)
  "Compare analyzed query meaning without scopes, plans or descriptor identity."
  (cond ((sql-jsonb-p form) (list :jsonb (sql-jsonb-output form)))
        ((sql-decimal-p form) (list :numeric (sql-numeric-coefficient form) (sql-numeric-scale form)))
        ((sql-expression-key-p form)
         (list :expression-key (sql-expression-key-value form)))
        ((sql-subplan-p form)
         (list :subplan (sql-subplan-kind form)
               (prepared-query-semantic-key (sql-subplan-prepared form))
               (subquery-semantic-key (sql-subplan-test form))))
        ((sql-record-p form)
         (list :record (subquery-semantic-key (sql-record-fields form))
               (subquery-semantic-key (sql-record-values form))))
        ((and (consp form) (eq (first form) :col)
              (getf (cddr form) :aggregate-key))
         (sql-expression-key-value (getf (cddr form) :aggregate-key)))
        ((and (consp form) (eq (first form) :whole-row))
         (list :whole-row (getf (cdddr form) :source-index)
               (subquery-semantic-key (third form))
               :type (expr-type form) :typmod (expr-typmod form)
               :fields (subquery-semantic-key (getf (cdddr form) :fields))))
        ((consp form) (cons (subquery-semantic-key (car form))
                            (subquery-semantic-key (cdr form))))
        ((and (vectorp form) (not (stringp form)))
         (list :vector (map 'list #'subquery-semantic-key form)))
        (t form)))

(defun prepared-query-semantic-key (prepared)
  ;; Capture analyzed query meaning without transient scope or plan identity.
  (list
   :query
   :ctes (mapcar #'cte-definition-semantic-key (getf prepared :ctes))
   :set-operation (getf prepared :set-operation)
   :arms (when (getf prepared :set-operation)
           (list (prepared-query-semantic-key (getf prepared :left))
                 (prepared-query-semantic-key (getf prepared :right))))
   :coercions (subquery-semantic-key (getf prepared :coercions))
   :targets (mapcar (lambda (target)
                      (list (getf target :name) (getf target :type) (getf target :typmod)
                            (subquery-semantic-key (getf target :expr))))
                    (getf prepared :targets))
   :relations
   (mapcar (lambda (relation)
             (list (pg-class-oid (getf relation :table))
                   (getf relation :alias) (getf relation :offset) (getf relation :width)
                   (cond ((getf relation :cte-worktable)
                          (list :worktable (sql-cte-name (getf relation :cte))))
                         ((getf relation :cte) (cte-definition-semantic-key (getf relation :cte))))
                   (getf relation :cte-levelsup)
                   (when (getf relation :subquery-prepared)
                     (prepared-query-semantic-key (getf relation :subquery-prepared)))))
           (getf prepared :source-relations))
   :where (subquery-semantic-key (getf prepared :where))
   :having (subquery-semantic-key (getf prepared :having))
   :has-having (getf prepared :has-having)
   :joins (subquery-semantic-key (getf prepared :joins))
   :group-keys (subquery-semantic-key (getf prepared :group-keys))
   :aggregates (mapcar (lambda (aggregate)
                         (list (getf aggregate :kind) (getf aggregate :type)
                               (subquery-semantic-key (getf aggregate :input))))
                       (getf prepared :aggregates))
   :order (subquery-semantic-key (getf prepared :order))
   :limit (getf prepared :limit)
   :windows (subquery-semantic-key (getf prepared :windows))
   :window-keys (subquery-semantic-key (getf prepared :window-keys))
   :distinct (getf prepared :distinct)
   :set-functions (subquery-semantic-key (getf prepared :set-functions))))

(defun analyze-sublink (expression scope)
  ;; Analyze against the enclosing scope before enforcing the output type boundary.
  (unless *select-sublinks-allowed-p*
    (ereport "0A000" "subqueries are not allowed in this expression"))
  (let* ((kind (second expression))
         (prepared (prepare-select-internal (third expression) scope))
         (targets (getf prepared :targets))
         (target (first targets))
         (type (getf target :type))
         (typmod (getf target :typmod))
         (descriptor (make-sql-subplan :kind kind :prepared prepared)))
    (unless (or (eq kind :exists) (= (length targets) 1))
      (ereport "42601" "subquery must return only one column"))
    (when (member kind '(:any :all))
      (let* ((left (analyze-expr (fourth expression) scope))
             (output (list :subquery-output :type type :typmod typmod))
             (operands (analyze-comparison-operands left output))
             (comparison (list :binop (fifth expression) (first operands) (second operands)
                               :type :bool :typmod -1)))
        (when (analyzed-expression-contains-set-p comparison)
          (ereport "42804" "row comparison operator must not return a set"))
        (setf (sql-subplan-test descriptor) comparison)))
    (when (eq kind :array)
      (setf type (or (sql-array-type type)
                     (ereport "0A000" "ARRAY subquery element type ~a is not supported" type))
            typmod -1))
    (when (member kind '(:exists :any :all))
      (setf type :bool typmod -1))
    (push descriptor *select-sublinks*)
    (list :subplan descriptor :type type :typmod typmod
          :result-name (case kind (:exists "exists") (:array "array")
                             (:scalar (getf target :name)) (t "?column?")))))
