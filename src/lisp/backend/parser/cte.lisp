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

(defun parse-with-select (p)
  (expect-kw p "with")
  (when (match-kw p "recursive") (ereport "0A000" "recursive CTEs are not supported"))
  (let ((definitions
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
    (unless (or (kw-p p "select") (punct-p p #\())
      (ereport "0A000" "WITH currently supports SELECT statements only"))
    (let ((query (parse-select-expression p)))
      (when (getf query :with) (syntax-error p "multiple WITH clauses"))
      (setf (getf query :with) definitions)
      query)))

(defun prepare-with-definitions (definitions)
  ;; Publish each binding only after analysis, so later definitions see earlier
  ;; ones while nested WITH clauses can shadow their enclosing environment.
  (let ((names nil) (result nil))
    (dolist (raw definitions)
      (when (member (getf raw :name) names :test #'equal)
        (ereport "42712" "WITH query name \"~a\" specified more than once" (getf raw :name)))
      (push (getf raw :name) names))
    (dolist (raw definitions (nreverse result))
      (let* ((definition (make-sql-cte :name (getf raw :name) :aliases (getf raw :aliases)
                                       :materialized (getf raw :materialized) :query (getf raw :query)
                                       :owner-level *select-cte-level*))
             (prepared (let ((*cte-definition-analysis-p* t))
                         (prepare-select-internal (sql-cte-query definition) (make-select-scope))))
             (aliases (sql-cte-aliases definition))
             (targets (getf prepared :targets)))
        (when (> (length aliases) (length targets))
          (ereport "42P10" "WITH query has fewer columns than its column alias list"))
        (loop for alias in aliases for target in targets do (setf (getf target :name) alias))
        (setf (sql-cte-prepared definition) prepared)
        (push (cons (sql-cte-name definition) definition) *select-cte-environment*)
        (push definition result)))))

(defun cte-source-definition (source)
  (when (stringp source)
    (cdr (assoc source *select-cte-environment* :test #'equal))))

(defun make-cte-relation-spec (definition alias offset)
  (incf (sql-cte-references definition))
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
          :cte definition :cte-filter nil :cte-reference-plan nil :cte-levelsup (- *select-cte-level* (sql-cte-owner-level definition)))))

(defun cte-function-volatile-p (name)
  "Use pg_proc.dat volatility for supported builtins; unknown helpers stay fenced."
  (and (not (member name
                    '("lower" "current_setting" "array_length"
                      "pg_get_expr" "pg_get_replica_identity_index"
                      "pg_stat_get_wal_receiver" "pg_get_publication_tables"
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
  ;; Volatile producers are shared even with NOT MATERIALIZED; otherwise the
  ;; policy and reference count determine whether readers share one producer.
  (dolist (definition (getf prepared :ctes))
    (setf (sql-cte-shared-p definition)
          (or (eq (sql-cte-materialized definition) :always)
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
