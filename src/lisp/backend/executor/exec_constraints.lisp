;;;; Persisted scalar expressions and CHECK enforcement.
(in-package #:pg)

(defun validate-stored-expression (form context)
  (labels ((walk (node)
             (when (consp node)
               (case (first node)
                 (:sublink (ereport "0A000" "cannot use subquery in ~a" context))
                 (:window (ereport "42P20" "window functions are not allowed in ~a" context))
                 (:column (when (eq context :default)
                            (ereport "0A000" "cannot use column reference in DEFAULT expression"))))
               (when (raw-aggregate-kind node)
                 (ereport "42803" "aggregate functions are not allowed in ~a" context))
               (walk (car node)) (walk (cdr node)))))
    (walk form)))

(defun stored-expression-sequences (form)
  "Validate sequence dependencies; NULL names have no dependency.
Only constant names can be tracked for persisted expressions and replication."
  (let ((names nil))
    (labels ((walk (node)
               (when (consp node)
                 (when (and (eq (first node) :func)
                            (member (second node) '("nextval" "currval" "setval") :test #'string=))
                   (let ((argument (first (third node))))
                     (unless (and (eq (first argument) :const)
                                  (or (stringp (second argument)) (eq (second argument) +null+)))
                       (ereport "0A000" "stored expressions require a constant sequence name"))
                     (when (stringp (second argument))
                       (let ((name (strip-schema (second argument))))
                         (unless (lookup-sequence name)
                           (ereport "42P01" "sequence ~a does not exist" name))
                         (pushnew name names :test #'string=)))))
                 (walk (car node)) (walk (cdr node)))))
      (walk form))
    names))

(defun fold-column-default (expression)
  "Copy a demanded default, folding immutable parts before row execution.
Conditional forms visit only live branches. Strict operators still fold every
argument before a constant NULL may remove stable or volatile calls; errors in
those arguments therefore occur before any row execution or sequence effects."
  (labels ((constant (value source)
             (list :const value :type (expr-type source) :typmod (expr-typmod source)))
           (constant-p (node) (and (consp node) (member (first node) '(:const :bool :scalar-array-const))))
           (null-p (node) (and (constant-p node) (eq (second node) +null+)))
           (finish (node)
             (multiple-value-bind (value known) (having-gate-value (having-gate-skeleton node))
               (if known (constant value node) node)))
           (fold (node)
             (unless (consp node) (return-from fold node))
             (case (first node)
               ((:const :bool :scalar-array-const) node)
               (:assignment
                (let ((copy (copy-list node)) (argument (fold (second node))))
                  (setf (second copy) argument)
                  (if (constant-p argument) (constant (eval-expr copy #()) node) copy)))
               (:case
                   (let ((branches nil) (default (third node)))
                     (dolist (branch (second node))
                       (let ((condition (fold (first branch))))
                         (cond ((constant-p condition)
                                (when (eq (second condition) t)
                                  (setf default (second branch)) (return)))
                               (t (push (list condition (fold (second branch))) branches)))))
                     (let ((copy (copy-list node)))
                       (setf (second copy) (nreverse branches) (third copy) (fold default))
                       (if branches copy (third copy)))))
               (:coalesce
                (let ((arguments nil))
                  (dolist (argument (second node))
                    (let ((folded (fold argument)))
                      (unless (null-p folded) (push folded arguments))
                      (when (and (constant-p folded) (not (null-p folded))) (return))))
                  (cond ((null arguments) (constant +null+ node))
                        ((null (cdr arguments)) (car arguments))
                        (t (let ((copy (copy-list node)))
                             (setf (second copy) (nreverse arguments)) copy)))))
               ((:and :or)
                (let* ((and-p (eq (first node) :and))
                       (left (fold (second node)))
                       (absorbing (if and-p nil t)))
                  (when (and (constant-p left) (eq (second left) absorbing))
                    (return-from fold (constant absorbing node)))
                  (let ((right (fold (third node))) (copy (copy-list node)))
                    (when (and (constant-p right) (eq (second right) absorbing))
                      (return-from fold (constant absorbing node)))
                    (setf (second copy) left (third copy) right)
                    (finish copy))))
               (otherwise
                (unless (member (first node)
                                '(:cast :unary :not :binop :concat :array-length :is-null :is-bool
                                  :array :func :any :all :in))
                  (return-from fold (mapcar #'fold node)))
                (let* ((copy (mapcar #'fold node))
                       (strict-inputs
                         (case (first copy)
                           (:binop (list (third copy) (fourth copy)))
                           ((:concat :array-length) (list (second copy) (third copy)))
                           ((:cast :not) (list (second copy)))
                           (:unary (list (third copy)))
                           (:func
                            (when (or (having-immutable-function-p (second copy))
                                      (member (second copy) '("nextval" "currval" "setval" "current_setting" "pg_promote"
                                                              "pg_get_expr" "pg_get_replica_identity_index"
                                                              "pg_cancel_backend" "pg_terminate_backend")
                                              :test #'string=))
                              (third copy))))))
                  (if (some #'null-p strict-inputs) (constant +null+ copy)
                      (finish copy)))))))
    (fold expression)))

(defun prepare-dml-column-defaults (prepared ast)
  ;; The catalog owns immutable expression trees. Cache folded copies only for
  ;; defaults demanded by this statement, after leaving catalog locks. Preserve
  ;; planner order: physical target list, physical conflict SET, then explicit
  ;; multi-row VALUES defaults in source order.
  (let ((columns (getf prepared :columns)) (defaults (make-hash-table :test #'eq))
                                           (values-defaults nil))
    (labels ((fold-needed (columns &optional (physical-order-p t))
               (dolist (column (if physical-order-p
                                   (sort (copy-list columns) #'< :key #'pg-column-attnum) columns))
                 (when (and (pg-column-default-expression column) (not (gethash column defaults)))
                   (setf (gethash column defaults)
                         (fold-column-default (pg-column-default-expression column)))))))
      (when (eq (getf ast :type) :insert)
        (let ((omitted (loop for column across columns for attnum from 1
                             unless (member attnum (getf prepared :attnums)) collect column))
              (explicit (loop for row in (getf prepared :rows)
                              append (loop for expression in row for attnum in (getf prepared :attnums)
                                           when (eq expression :default) collect (aref columns (1- attnum))))))
          ;; A multi-row VALUES is a source RTE, planned after outer omitted
          ;; defaults. Its explicit DEFAULT expressions retain source order.
          (if (cdr (getf prepared :rows))
              (progn (fold-needed omitted) (setf values-defaults explicit))
              (fold-needed (append omitted explicit)))))
      (fold-needed (loop for assignment in (getf prepared :sets)
                         when (eq (cdr assignment) :default) collect (car assignment)))
      ;; planner.c preprocesses targetList before onConflictSet.
      (fold-needed (loop for assignment in (getf (getf prepared :conflict) :sets)
                         when (eq (getf (cdr assignment) :expr) :default) collect (car assignment)))
      (fold-needed values-defaults nil))
    (setf (getf prepared :column-defaults) defaults)
    prepared))

(defun canonical-stored-expression (expression original raw table)
  "Return replay SQL and raw AST, preserving cooked temporal constants."
  (if (not (catalog-temporal-constant-p expression))
      (values original raw)
      (let* ((sql (catalog-canonical-expression-sql expression table))
             (statements (parse-statements (concatenate 'string "SELECT " sql)))
             (target (and (= (length statements) 1) (first (getf (first statements) :targets))))
             (parsed (and target (getf target :expr))))
        (unless parsed (ereport "0A000" "cannot replay stored expression"))
        (let* ((*select-aggregate-analysis* nil)
               (*select-set-functions-allowed-p* nil)
               (reanalyzed (analyze-expr parsed table))
               (replayed-sql (catalog-canonical-expression-sql reanalyzed table)))
          (unless (string= sql replayed-sql)
            (ereport "0A000" "stored expression cannot be replayed faithfully")))
        (values sql parsed))))

(defun cook-column-default (raw column)
  (validate-stored-expression raw :default)
  (let* ((*select-aggregate-analysis* nil)
         (*select-set-functions-allowed-p* nil)
         (expression (analyze-expr raw nil)))
    (when (analyzed-expression-contains-set-p expression)
      (ereport "0A000" "set-returning functions are not allowed in DEFAULT"))
    (stored-expression-sequences expression)
    (analyze-subquery-assignment expression column)))

(defun table-constraint-name-used-p (table name)
  (or (find name (pg-class-checks table) :key (lambda (check) (getf check :name)) :test #'string=)
      (find name (remove-if-not #'pg-index-constraint-owned-p (table-indexes table))
            :key #'pg-index-name :test #'string=)))

(defun check-constraint-default-name (table expression)
  (let ((columns nil))
    (labels ((walk (node)
               (when (consp node)
                 (when (eq (first node) :col)
                   (pushnew (second node) columns))
                 (walk (car node)) (walk (cdr node)))))
      (walk expression))
    (let* ((base (if (= 1 (length columns))
                     (format nil "~a_~a_check" (pg-class-name table)
                             (pg-column-name (aref (pg-class-columns table) (1- (first columns)))))
                     (format nil "~a_check" (pg-class-name table)))))
      (loop for suffix from 0
            for name = (if (zerop suffix) base (format nil "~a~d" base suffix))
            unless (table-constraint-name-used-p table name) return name))))

(defun cook-check-constraint (table raw)
  (validate-stored-expression (getf raw :expression) :check)
  (labels ((check-qualifier (node)
             (when (consp node)
               (when (and (eq (first node) :column) (third node)
                          (not (string= (second node) (pg-class-name table))))
                 (ereport "42P01" "missing FROM-clause entry for table ~s" (second node)))
               (check-qualifier (car node)) (check-qualifier (cdr node)))))
    (check-qualifier (getf raw :expression)))
  (let* ((*select-set-functions-allowed-p* nil)
         (expression (coerce-boolean-expression (analyze-expr (getf raw :expression) table) "CHECK"))
         (name (or (getf raw :name) (check-constraint-default-name table expression))))
    (when (analyzed-expression-contains-set-p expression)
      (ereport "0A000" "set-returning functions are not allowed in CHECK"))
    (when (table-constraint-name-used-p table name)
      (ereport "42710" "constraint ~a for relation ~a already exists" name (pg-class-name table)))
    (stored-expression-sequences expression)
    (multiple-value-bind (sql parsed)
        (canonical-stored-expression expression (getf raw :sql) (getf raw :expression) table)
      (setf (getf raw :sql) sql (getf raw :expression) parsed)
      (list :name name :expression expression :sql sql
            :validated (not (getf raw :not-valid))))))

(defun check-table-constraints (table values &optional (checks (pg-class-checks table)))
  "Like ExecCheck, NULL passes. Check names determine evaluation order.
NOT VALID checks still apply to new row versions, after their NOT NULL checks."
  (dolist (check (sort (copy-list checks) #'string< :key (lambda (check) (getf check :name))))
    (when (null (eval-expr (getf check :expression) values))
      (ereport "23514" "new row for relation ~s violates check constraint ~s"
               (pg-class-name table) (getf check :name)))))

(defun install-table-expressions (table column-definitions constraints)
  (loop for column across (pg-class-columns table) for definition in column-definitions
        when (getf definition :default-expression)
          do (let ((cooked (cook-column-default (getf definition :default-expression) column)))
               (multiple-value-bind (sql raw)
                   (canonical-stored-expression cooked (getf definition :default-sql)
                                                (getf definition :default-expression) table)
                 (setf (pg-column-default-expression column) cooked
                       (pg-column-default-sql column) sql
                       (getf definition :default-expression) raw
                       (getf definition :default-sql) sql))))
  (dolist (raw (append (loop for definition in column-definitions append (getf definition :checks))
                       (remove-if-not (lambda (raw) (eq (getf raw :kind) :check)) constraints)))
    (push (cook-check-constraint table raw) (pg-class-checks table))))

(defmacro with-stored-expression-barrier ((table action) &body body)
  (let ((run (gensym "CHANGE-")))
    `(flet ((,run () ,@body))
       (if (eq ,action :validate-constraint)
           ;; VALIDATE, like C's ShareUpdateExclusiveLock, permits DML. The
           ;; already installed NOT VALID check guards every new row version.
           (,run)
           (with-relation-build-locks ((list (pg-class-relfilenode ,table)) t)
             (,run))))))

(defun exec-alter-stored-expression (stream db table ast)
  "Alter stored expressions under schema DDL serialization.
ADD, default, drop and rename actions wait for relation users. VALIDATE permits
DML: the installed NOT VALID check protects new versions while a fresh retained
snapshot is scanned. Expression evaluation holds no global write/catalog mutex."
  (with-schema-write-lock
    (with-stored-expression-barrier (table (getf ast :action))
      (let* ((*current-snapshot* (refresh-statement-snapshot))
             (action (getf ast :action))
             (column nil) (expression nil) (check nil)
             (name (getf ast :constraint-name)))
        (with-write-catalog-locks
          (ensure-current-table table)
          (case action
            ((:set-default :drop-default)
             (setf column (find-column table (getf ast :column)))
             (unless column (ereport "42703" "column ~s does not exist" (getf ast :column)))
             (when (eq action :set-default)
               (setf expression (cook-column-default (getf ast :expression) column))
               (multiple-value-bind (sql parsed)
                   (canonical-stored-expression expression (getf ast :sql) (getf ast :expression) table)
                 (setf (getf ast :sql) sql (getf ast :expression) parsed))))
            (:add-check (setf check (cook-check-constraint table (getf ast :check))))
            (otherwise
             (setf check (find name (pg-class-checks table)
                               :key (lambda (check) (getf check :name)) :test #'string=))
             (unless check
               (when (and (eq action :validate-constraint) (table-constraint-name-used-p table name))
                 (ereport "42809" "constraint ~s is not a CHECK constraint" name))
               (if (and (eq action :drop-constraint) (getf ast :constraint-if-exists))
                   (progn
                     (send-notice stream (format nil "constraint ~s does not exist, skipping" name))
                     (return-from exec-alter-stored-expression t))
                   (ereport "42704" "constraint ~s does not exist" name)))
             (when (and (eq action :rename-constraint)
                        (table-constraint-name-used-p table (getf ast :new-name)))
               (ereport "42710" "constraint ~s already exists" (getf ast :new-name))))))
        (when (or (and (eq action :add-check) (getf check :validated))
                  (and (eq action :validate-constraint) (not (getf check :validated))))
          (do-table-tuples (tuple table)
            (check-for-interrupts)
            (check-table-constraints table (htup-values tuple) (list check))))
        (with-write-catalog-locks
          (ensure-current-table table)
          (with-catalog-mutation-rollback (db)
            (case action
              ((:set-default :drop-default)
               (let ((replacement (copy-pg-column column))
                     (columns (copy-seq (pg-class-columns table))))
                 (setf (pg-column-default-expression replacement) expression
                       (pg-column-default-sql replacement) (and expression (getf ast :sql))
                       (pg-column-default-sequence-name replacement) nil
                       (aref columns (1- (pg-column-attnum column))) replacement
                       (pg-class-columns table) columns)))
              (:add-check (setf (pg-class-checks table) (cons check (pg-class-checks table))))
              (:drop-constraint
               (setf (pg-class-checks table) (remove check (pg-class-checks table))))
              ((:validate-constraint :rename-constraint)
               (let ((replacement (copy-list check)))
                 (if (eq action :validate-constraint)
                     (setf (getf replacement :validated) t)
                     (setf (getf replacement :name) (getf ast :new-name)))
                 (setf (pg-class-checks table)
                       (substitute replacement check (pg-class-checks table))))))
            (save-catalog :logical-event (logical-catalog-event-for-ast ast)))))))
  t)
