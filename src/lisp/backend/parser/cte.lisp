;;;; cte.lisp -- lexical WITH definitions and shared producer identities.
(in-package #:pg)

(defstruct sql-cte name aliases materialized query prepared plan (references 0)
  owner-level dependencies shared-p)

(defvar *select-cte-environment* nil)
(defvar *select-cte-level* 0)
(defvar *cte-pull-planning-p* nil)

(defvar *cte-definition-analysis-p* nil)

(defvar *cte-retain-window-junk-p* nil "Retain unused named-window outputs in shared CTE producer plans.")

(defvar *cte-execution-context* nil)
(defvar *cte-open-states* nil)

(defvar *cte-active-plan* nil)

(defvar *sql-open-iterators* nil)

(defvar *recursive-cte-worktables* nil)

(defvar *recursive-cte-generation-caches* nil)

(defun prepared-query-worktable-cache (prepared)
  ;; Select the nearest active generation actually referenced by this query.
  ;; The visited set is needed because prepared CTE graphs can be cyclic.
  (declare (notinline sql-subplan-prepared sql-subplan-test))
  (unless *recursive-cte-generation-caches* (return-from prepared-query-worktable-cache nil))
  (let ((seen (make-hash-table :test #'eq)) (found nil))
    (labels ((expression (form)
               (when (consp form)
                 (if (subquery-expression-p form)
                     (progn (expression (sql-subplan-test (second form)))
                            (query (sql-subplan-prepared (second form))))
                     (progn (expression (car form)) (expression (cdr form))))))
             (query (state)
               (unless (gethash state seen)
                 (setf (gethash state seen) t)
                 (if (getf state :set-operation)
                     (progn (query (getf state :left)) (query (getf state :right)))
                     (progn
                       (dolist (key '(:where :having :group-keys :window-keys :windows :order :set-functions))
                         (expression (getf state key)))
                       (dolist (target (getf state :targets)) (expression (getf target :expr)))
                       (dolist (aggregate (getf state :aggregates)) (expression (getf aggregate :input)))
                       (dolist (join (getf state :joins)) (expression (getf join :qual)))
                       (dolist (relation (getf state :source-relations (getf state :relations)))
                         (cond
                           ((getf relation :cte-worktable)
                            (when (assoc (getf relation :cte) *recursive-cte-worktables* :test #'eq)
                              (pushnew (getf relation :cte) found :test #'eq)))
                           ((getf relation :cte) (query (sql-cte-prepared (getf relation :cte))))
                           ((getf relation :subquery-prepared) (query (getf relation :subquery-prepared))))))))))
      (query prepared))
    (loop for binding in *recursive-cte-generation-caches*
          when (member (car binding) found :test #'eq) return (cdr binding))))

(defvar *select-recursive-definition* nil)

(defvar *select-recursive-worktables* nil)

(defun raw-select-query-p (form)
  (and (consp form)
       (loop for (key value) on form by #'cddr
               thereis (and (eq key :type) (eq value :select)))))

(defun map-raw-cte-references (function query &optional hidden (context :okay))
  ;; Follow raw WITH scope before name resolution, as parse_cte.c does.  Only
  ;; unqualified FROM names can denote CTEs; expression sublinks forbid self use.
  (labels
      ((nullable-context (current)
         (if (eq current :okay) :outer-join current))
       (source (item names current)
         (cond ((stringp item)
                (unless (member item names :test #'equal) (funcall function item current)))
               ((sql-derived-relation-p item)
                (query (sql-derived-relation-query item) names current))
               ((consp item)
                (let* ((join (second item)) (kind (getf join :type)))
                  (source (first item) names
                          (if (member kind '(:right :full)) (nullable-context current) current))
                  (source (getf join :table) names
                          (if (member kind '(:left :full)) (nullable-context current) current))
                  (expression (getf join :condition) names current)))))
       (expression (form names current)
         (cond ((atom form) nil)
               ((raw-select-query-p form) (query form names current))
               ((eq (first form) :sublink)
                (query (third form) names :subquery)
                (expression (fourth form) names current)
                (expression (fifth form) names current))
               (t (dolist (part form) (expression part names current)))))
       (body (state names current)
         (if (getf state :set-operation)
             (progn (query (getf state :left) names current)
                    (query (getf state :right) names current))
             (let ((tree (getf state :from)))
               (dolist (join (getf state :joins)) (setf tree (list tree join)))
               (source tree names current)))
         (dolist (key '(:targets :where :group-by :having :windows :order-by))
           (expression (getf state key) names current)))
       (query (state names current)
         (when state
           (let ((definitions (getf state :with)))
             (if (getf state :with-recursive)
                 (let ((local (append (mapcar (lambda (raw) (getf raw :name)) definitions) names)))
                   (dolist (raw definitions) (query (getf raw :query) local current))
                   (body state local current))
                 (let ((local names))
                   (dolist (raw definitions)
                     (query (getf raw :query) local current)
                     (push (getf raw :name) local))
                   (body state local current)))))))
    (query query hidden context)))

(defun order-recursive-with-definitions (definitions)
  ;; Order forward dependencies before analysis; self edges are handled by the
  ;; recursive worktable, while cycles between definitions are unsupported.
  (let* ((definitions (mapcar (lambda (raw) (list* :recursive-p nil raw)) definitions))
         (names (mapcar (lambda (raw) (getf raw :name)) definitions))
         (pending (copy-list definitions))
         (dependencies (make-hash-table :test #'eq))
         (ordered nil))
    (dolist (raw definitions)
      (let ((needed nil) (self nil))
        (map-raw-cte-references
         (lambda (name context)
           (declare (ignore context))
           (when (member name names :test #'equal)
             (if (equal name (getf raw :name)) (setf self t)
                 (pushnew name needed :test #'equal))))
         (getf raw :query))
        (setf (gethash raw dependencies) needed
              (getf raw :recursive-p) self)))
    (loop while pending
          for next = (find-if (lambda (raw) (null (gethash raw dependencies))) pending)
          do (unless next (ereport "0A000" "mutual recursion between WITH items is not implemented"))
             (setf pending (remove next pending :test #'eq))
             (push next ordered)
             (dolist (raw pending)
               (setf (gethash raw dependencies)
                     (remove (getf next :name) (gethash raw dependencies) :test #'equal))))
    (nreverse ordered)))

(defun validate-recursive-cte (raw)
  (let ((query (getf raw :query)) (name (getf raw :name)) (count 0))
    (unless (member (getf query :set-operation) '(:union :union-all))
      (ereport "42P19" "recursive query ~s does not have the form non-recursive-term UNION [ALL] recursive-term" name))
    (labels ((reference (source context)
               (when (equal source name)
                 (unless (eq context :okay)
                   (ereport "42P19" "recursive reference to query ~s is not allowed in ~a" name context))
                 (when (> (incf count) 1)
                   (ereport "42P19" "recursive reference to query ~s must not appear more than once" name)))))
      ;; A WITH belonging to the UNION itself cannot contain a recursive use.
      (when (getf query :with)
        (let ((wrapper (copy-list query)))
          (setf (getf wrapper :left) nil (getf wrapper :right) nil
                (getf wrapper :targets) nil (getf wrapper :order-by) nil)
          (map-raw-cte-references #'reference wrapper nil :non-recursive-term)))
      (when (getf query :order-by)
        (ereport "0A000" "ORDER BY in a recursive query is not implemented"))
      (when (getf query :limit)
        (ereport "0A000" "LIMIT in a recursive query is not implemented"))
      (map-raw-cte-references #'reference (getf query :left) nil :non-recursive-term)
      (map-raw-cte-references #'reference (getf query :right))
      (unless (= count 1)
        (ereport "42P19" "recursive query ~s has no recursive reference in its recursive term" name)))))

(defun apply-cte-column-aliases (targets aliases)
  (when (> (length aliases) (length targets))
    (ereport "42P10" "WITH query has fewer columns than its column alias list"))
  (loop for alias in aliases for target in targets do (setf (getf target :name) alias))
  targets)

(defun freeze-recursive-cte-targets (targets aliases)
  ;; Worktable columns are fixed from the non-recursive arm.  In particular,
  ;; unknown literals become text before the recursive arm is analyzed.
  (let ((frozen (mapcar #'copy-list targets)))
    (dolist (target frozen)
      (when (in-unknown-constant-p (getf target :expr))
        (setf (getf target :expr)
              (append (coerce-common-expression (getf target :expr) :text) (list :explicit-cast t))
              (getf target :type) :text (getf target :typmod) -1))
      (setf (getf target :table-oid) 0 (getf target :attnum) 0))
    (apply-cte-column-aliases frozen aliases)))

(defun validate-recursive-term-aggregates (prepared)
  ;; C rejects aggregates at a query level with a direct self-reference RTE.
  (map-prepared-query-expressions
   (lambda (form depth) (declare (ignore depth)) form)
   prepared 0
   (lambda (state depth)
     (declare (ignore depth))
     (when (and (plusp (getf state :aggregate-count 0))
                (some (lambda (relation) (getf relation :cte-worktable))
                      (getf state :source-relations (getf state :relations))))
       (ereport "42P19" "aggregate functions are not allowed in a recursive query's recursive term"))))
  prepared)

(defun parse-with-select (p)
  (expect-kw p "with")
  (let* ((recursive-p (match-kw p "recursive"))
         (definitions
           (loop collect
                 (let* ((name (pident p))
                        (aliases (when (punct-p p #\()
                                   (with-parens (p) (parse-ident-list p)))))
                   (expect-kw p "as")
                   (let ((policy (cond ((match-kw p "materialized") :always)
                                       ((match-kw p "not") (expect-kw p "materialized") :never))))
                     (list :name name :aliases aliases :materialized policy
                           :query (with-parens (p)
                                    (when (member (pval p) '("insert" "update" "delete" "merge")
                                                  :test #'equal)
                                      (ereport "0A000" "data-modifying CTEs are not supported"))
                                    (parse-select-expression p)))))
                 while (match-punct p #\,))))
    (when (or (kw-p p "search") (kw-p p "cycle"))
      (ereport "0A000" "recursive SEARCH and CYCLE clauses are not supported"))
    ;; VALUES is a SELECT operand, so it can follow WITH directly.
    (unless (or (kw-p p "select") (kw-p p "values") (punct-p p #\()
                (kw-p p "insert") (kw-p p "update") (kw-p p "delete"))
      (ereport "0A000" "unsupported statement after WITH"))
    (let ((query (if (or (kw-p p "insert") (kw-p p "update") (kw-p p "delete"))
                     (parse-statement p) (parse-select-expression p))))
      (when (getf query :with) (syntax-error p "multiple WITH clauses"))
      (setf (getf query :with) definitions
            (getf query :with-recursive) recursive-p)
      query)))

(defun prepare-with-definitions (definitions &optional recursive-p)
  ;; Nonrecursive WITH publishes bindings sequentially; RECURSIVE first orders
  ;; forward references, and publishes a seed-shaped worktable for each self use.
  (let ((names nil) (result nil))
    (dolist (raw definitions)
      (when (member (getf raw :name) names :test #'equal)
        (ereport "42712" "WITH query name \"~a\" specified more than once" (getf raw :name)))
      (push (getf raw :name) names))
    (dolist (raw (if recursive-p (order-recursive-with-definitions definitions) definitions)
                 (nreverse result))
      (let* ((self-p (and recursive-p (getf raw :recursive-p)))
             (definition (make-sql-cte :name (getf raw :name) :aliases (getf raw :aliases)
                                       :materialized (getf raw :materialized) :query (getf raw :query)
                                       :owner-level *select-cte-level*)))
        (when self-p
          (validate-recursive-cte raw)
          (push (cons (sql-cte-name definition) definition) *select-cte-environment*))
        (let* ((*cte-definition-analysis-p* t)
               (*select-recursive-definition* (and self-p definition))
               (prepared (prepare-select-internal (sql-cte-query definition) (make-select-scope))))
          (apply-cte-column-aliases (getf prepared :targets) (sql-cte-aliases definition))
          (setf (sql-cte-prepared definition) prepared))
        (unless self-p
          (push (cons (sql-cte-name definition) definition) *select-cte-environment*))
        (push definition result)))))

(defun cte-source-definition (source)
  (when (stringp source)
    (cdr (assoc source *select-cte-environment* :test #'equal))))

(defun make-cte-relation-spec (definition alias offset)
  (unless (member definition *select-recursive-worktables* :test #'eq)
    (incf (sql-cte-references definition)))
  (let* ((targets (getf (sql-cte-prepared definition) :targets))
         (name (or alias (sql-cte-name definition)))
         (table (make-pg-class
                 :oid 0 :name name
                 :columns (coerce
                           (loop for target in targets for slot from 1
                                 collect (make-pg-column :name (getf target :name)
                                                         :type (getf target :type)
                                                         :typmod (getf target :typmod)
                                                         :attnum slot))
                           'vector))))
    (list :name (sql-cte-name definition) :alias alias :table table :offset offset
          :width (length targets) :columns (pg-class-columns table) :virtual-p t :origins targets
          :cte definition :cte-worktable (not (null (member definition *select-recursive-worktables* :test #'eq)))
          :cte-filter nil :cte-reference-plan nil :cte-levelsup (- *select-cte-level* (sql-cte-owner-level definition)))))

(defun cte-function-volatile-p (name)
  "Use pg_proc.dat volatility for supported builtins; unknown helpers stay fenced."
  (and (not (member name
                    '("lower" "current_setting" "array_length"
                      "pg_get_expr" "pg_get_replica_identity_index"
                      "pg_stat_get_wal_receiver" "pg_get_publication_tables" "pg_get_replication_slots"
                      "pg_wal_lsn_diff" "pg_walfile_name")
                    :test #'string=))
       (analyzed-expression-volatile-p (list :func name nil))))

(defun cte-expression-volatile-p (form)
  ;; SQL-SUBPLAN is defined by the following parser module.
  (declare (notinline sql-subplan-prepared sql-subplan-test))
  (and (consp form)
       (or (and (subquery-expression-p form)
                (or (cte-expression-volatile-p (sql-subplan-test (second form)))
                    (cte-query-volatile-p (sql-subplan-prepared (second form)))))
           (and (member (first form) '(:func :set-func :single-record))
                (cte-function-volatile-p (second form)))
           (cte-expression-volatile-p (car form))
           (cte-expression-volatile-p (cdr form)))))

(defun cte-query-volatile-p (prepared)
  (let ((volatile (cte-expression-volatile-p (getf prepared :window-validation-keys))))
    (map-prepared-query-expressions
     (lambda (form level)
       (declare (ignore level))
       (when (and (member (first form) '(:func :set-func :single-record))
                  (cte-function-volatile-p (second form)))
         (setf volatile t))
       form)
     prepared 0
     (lambda (state level)
       (declare (ignore level))
       (when (cte-expression-volatile-p (getf state :window-validation-keys))
         (setf volatile t))))
    volatile))

(defun plan-query-ctes (prepared)
  ;; Recursive and volatile producers are shared even with NOT MATERIALIZED;
  ;; otherwise policy and reference count determine whether readers share one.
  (dolist (definition (getf prepared :ctes))
    (setf (sql-cte-shared-p definition)
          (or (getf (sql-cte-prepared definition) :recursive-cte)
              (eq (sql-cte-materialized definition) :always)
              (cte-query-volatile-p (sql-cte-prepared definition))
              (and (not (eq (sql-cte-materialized definition) :never))
                   (> (sql-cte-references definition) 1))))
    (when (and (plusp (sql-cte-references definition)) (null (sql-cte-plan definition)))
      (setf (getf (sql-cte-prepared definition) :skip-unused-window-validation)
            (not (sql-cte-shared-p definition)))
      (let ((*cte-pull-planning-p* t)
            (*cte-retain-window-junk-p* (sql-cte-shared-p definition)))
        (setf (sql-cte-plan definition) (plan-prepared-select (sql-cte-prepared definition)))))
    (setf (sql-cte-dependencies definition)
          (prepared-query-outer-depths (sql-cte-prepared definition))))
  prepared)

(defun cte-definition-semantic-key (definition)
  (list (sql-cte-name definition) (sql-cte-aliases definition)
        (sql-cte-materialized definition)
        (prepared-query-semantic-key (sql-cte-prepared definition))))

(defun wrap-query-cte-plan (tree prepared)
  (if (getf prepared :ctes) (list :with-ctes (getf prepared :ctes) tree) tree))

(defun cte-reference-filter-p (form relation)
  (cond ((atom form) t)
        ((member (first form) '(:subplan :outer-ref :window-ref)) nil)
        ((eq (first form) :col)
         (< (getf relation :offset) (second form)
            (1+ (+ (getf relation :offset) (getf relation :width)))))
        ((and (eq (first form) :func) (cte-expression-volatile-p form)) nil)
        (t (and (cte-reference-filter-p (car form) relation)
                (cte-reference-filter-p (cdr form) relation)))))

(defun substitute-cte-output-columns (form relation)
  (cond ((atom form) form)
        ((eq (first form) :col)
         (copy-tree (getf (nth (- (second form) (getf relation :offset) 1)
                               (getf (sql-cte-prepared (getf relation :cte)) :targets))
                          :expr)))
        (t (cons (substitute-cte-output-columns (car form) relation)
                 (substitute-cte-output-columns (cdr form) relation)))))

(defun cte-reference-producer-tree (tree qual)
  (cond ((null qual) tree)
        ((eq (first tree) :with-ctes)
         (list :with-ctes (second tree) (cte-reference-producer-tree (third tree) qual)))
        ((eq (first tree) :sort)
         (list :sort (second tree) (cte-reference-producer-tree (third tree) qual)))
        ((eq (first tree) :project)
         (list :project (second tree) (list :filter qual (third tree))))
        (t (error "CTE reference filter crossed an unsupported query boundary"))))
