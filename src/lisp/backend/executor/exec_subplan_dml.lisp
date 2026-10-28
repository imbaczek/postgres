;;;; exec_subplan_dml.lisp -- statement-owned subqueries in mutation expressions.
(in-package #:pg)

(defun raw-query-sublinks-p (form)
  (and (consp form)
       (or (eq (first form) :sublink)
           (raw-query-sublinks-p (car form))
           (raw-query-sublinks-p (cdr form)))))

(defun analyze-subquery-assignment (expression column)
  (let ((source (expr-type expression))
        (target (pg-column-type column))
        (typmod (pg-column-typmod column)))
    (when (in-unknown-constant-p expression)
      (setf expression (coerce-in-constant expression target (if (eq target :interval) typmod -1))
            source target))
    (unless (or (sql-implicit-coercion-p source target)
                (member target '(:text :varchar))
                (and (member target '(:int2 :int4 :int8 :numeric :oid :float4 :float8))
                     (member source '(:int2 :int4 :int8 :numeric :oid :float4 :float8))))
      (ereport "42804" "column \"~a\" is of type ~a but expression is of type ~a"
               (pg-column-name column) target source))
    (list :assignment expression :type target :typmod typmod)))

(defun eval-subquery-assignment (expression environment)
  (let* ((argument (second expression))
         (target (expr-type expression))
         (typmod (expr-typmod expression))
         (value (sql-cast-value (eval-expr argument environment)
                                (expr-type argument) target (if (eq target :varchar) -1 typmod))))
    (when (and (eq target :varchar) (not (eq value +null+)) (>= typmod 4))
      (let ((maximum (- typmod 4)))
        (when (> (length value) maximum)
          (unless (every (lambda (character) (char= character #\Space))
                         (subseq value maximum))
            (ereport "22001" "value too long for type character varying(~a)" maximum))
          (setf value (subseq value 0 maximum)))))
    value))

(defun prepare-dml-returning (ast table &optional source)
  (when (getf ast :returning)
    (let* ((base (make-base-select-scope (getf ast :table) (getf ast :alias) table))
           (width (length (pg-class-columns table)))
           (scope (extend-dml-source-scope base source (* 3 width)))
           (columns (select-scope-columns scope))
           (options (getf ast :returning-options))
           (old-kind (getf *select-analysis-frame* :kind)))
      (loop for kind in '(:old :new)
            for offset in (list width (* 2 width))
            for implicit in '("old" "new")
            for explicit = (getf options kind)
            for name = (or explicit implicit)
            for collision = (find name (select-scope-relations scope)
                                  :key #'select-relation-qualifier :test #'equal)
            do (when (and explicit collision)
                 (ereport "42712" "table name ~s specified more than once" name))
               (unless collision
                 (setf scope
                       (extend-select-scope
                        scope (make-select-relation
                               :name (getf ast :table) :alias name :table table
                               :offset offset :width width)))))
      ;; OLD/NEW remain qualified-only; the current row and sources expand bare *.
      (setf (select-scope-columns scope) columns
            (select-scope-width scope) (+ (* 3 width) (if source (select-scope-width (getf source :scope)) 0)))
      (unwind-protect
           (progn
             (setf (getf *select-analysis-frame* :kind) :returning)
             (let ((targets (analyze-targets (getf ast :returning) scope)))
               (unless targets (ereport "42601" "RETURNING must have at least one column"))
               (when (some (lambda (target)
                             (analyzed-expression-contains-set-p (getf target :expr))) targets)
                 (ereport "0A000" "set-returning functions are not allowed in RETURNING"))
               targets))
        (setf (getf *select-analysis-frame* :kind) old-kind)))))

(defvar *dml-returning-spool* nil)

(defun send-dml-returning-description (stream targets)
  (when (and targets (not *dml-returning-spool*))
    (send-row-description stream (mapcar #'target->result-column targets))))

(defun emit-dml-returning (stream targets values &key old-values (new-values values) (source-values #()))
  ;; Evaluate against the mutation's row now, while its original snapshot is active.
  ;; The spool owns a copy until the entire command succeeds.
  (when targets
    (let* ((width (length values))
           (nulls (make-array width :initial-element +null+))
           (environment (concatenate 'vector values (or old-values nulls) (or new-values nulls) source-values))
           (row (call-with-dml-expression-evaluation
                 (lambda () (make-select-result-row environment nil targets)))))
      (if *dml-returning-spool*
          (query-row-spool-append *dml-returning-spool* (copy-query-sort-value (car row)))
          (send-select-row stream row targets)))))

(defun finish-dml-command (stream targets tag)
  ;; Like FillPortalStore for PORTAL_ONE_RETURNING, finish all mutations before
  ;; publishing rows; a later statement error must not expose partial results.
  (check-for-interrupts)
  (when (and targets *dml-returning-spool*)
    (query-row-spool-seal *dml-returning-spool*)
    (send-row-description stream (mapcar #'target->result-column targets))
    (map-query-row-spool *dml-returning-spool*
                         (lambda (values index)
                           (declare (ignore index))
                           (check-for-interrupts)
                           (send-select-row stream (cons values values) targets))))
  (send-command-complete stream tag))

(defun prepare-dml-subqueries (ast)
  (let ((prepared
          (with-write-lock
            (let* ((table (resolve-table-or-error (getf ast :table)))
                   (columns (pg-class-columns table))
                   (*select-outer-scope* nil)
                   (*select-cte-environment* nil)
                   (*select-cte-level* 1)
                   (definitions (prepare-with-definitions (getf ast :with) (getf ast :with-recursive)))
                   (*select-analysis-frame*
                     (list :parent nil :descriptors (make-array 0 :adjustable t :fill-pointer 0)
                           :group-keys nil :kind :dml :inside-input nil))
                   (*select-aggregate-analysis* *select-analysis-frame*)
                   (*select-sublinks-allowed-p* t)
                   (*select-sublinks* nil)
                   (*select-set-functions-allowed-p* nil)
                   (*inside-aggregate-input-p* nil)
                   (*aggregate-order-mode* nil)
                   (base-scope (if (eq (getf ast :type) :insert) (make-select-scope)
                                   (make-base-select-scope (getf ast :table) (getf ast :alias) table)))
                   (dml-source (when (getf ast :dml-source)
                                 (prepare-dml-source (getf ast :dml-source) base-scope)))
                   (scope (extend-dml-source-scope base-scope dml-source (length columns)))
                   (rows nil) (sets nil) (attnums nil) (targets nil)
                   (source nil) (source-assignments nil)
                   (where (when (getf ast :where)
                            (coerce-boolean-expression (analyze-expr (getf ast :where) scope) "WHERE"))))
              (labels ((assignment (raw column)
                         (let ((expression (if (eq (first raw) :default) :default
                                               (analyze-subquery-assignment (analyze-expr raw scope) column))))
                           (push (list :expr expression) targets)
                           expression))
                       (check-arity (width explicit)
                         (when (or (> width (length attnums))
                                   (and explicit (< width (length attnums))))
                           (ereport "42601" "INSERT has mismatched target and expression counts"))
                         (unless explicit (setf attnums (subseq attnums 0 width)))))
                (when (eq (getf ast :type) :insert)
                  (let ((names (getf ast :columns)))
                    (when (and names (/= (length names)
                                         (length (remove-duplicates names :test #'string=))))
                      (ereport "42701" "column specified more than once"))
                    (setf attnums (if names
                                      (mapcar (lambda (name) (pg-column-attnum (resolve-column-or-error table name))) names)
                                      (loop for index from 1 to (length columns) collect index)))
                    (cond
                      ((getf ast :source)
                       (setf source (prepare-select-internal (getf ast :source) nil))
                       (let ((outputs (getf source :targets)))
                         (check-arity (length outputs) names)
                         (setf source-assignments
                               (loop for target in outputs for attnum in attnums for slot from 1
                                     for expression = (if (in-unknown-constant-p (getf target :expr))
                                                          (copy-tree (getf target :expr))
                                                          (projected-target-ref target slot))
                                     for item = (list :expr (analyze-subquery-assignment expression
                                                                                         (aref columns (1- attnum))))
                                     do (push item targets)
                                     collect item))))
                      ((getf ast :default-values) (setf rows (list nil) attnums nil))
                      (t
                       (let* ((raw-rows (getf ast :rows)) (width (length (first raw-rows))))
                         (check-arity width names)
                         (setf rows
                               (mapcar (lambda (row)
                                         (unless (= (length row) width)
                                           (ereport "42601" "VALUES lists must all be the same length"))
                                         (loop for raw in row for attnum in attnums
                                               collect (assignment raw (aref columns (1- attnum)))))
                                       raw-rows)))))))
                (let ((names (mapcar (lambda (item) (getf item :column)) (getf ast :sets))))
                  (unless (= (length names) (length (remove-duplicates names :test #'equal)))
                    (ereport "42601" "multiple assignments to same column")))
                (setf sets
                      (mapcar (lambda (item)
                                (let ((column (resolve-column-or-error table (getf item :column))))
                                  (cons column (assignment (getf item :expr) column))))
                              (getf ast :sets))))
              (let* ((conflict (when (getf ast :on-conflict)
                                 (progn (setf (getf *select-analysis-frame* :kind) :on-conflict)
                                        (prepare-on-conflict table (getf ast :on-conflict) (getf ast :alias)))))
                     (returning (prepare-dml-returning ast table dml-source)))
                (list :ctes definitions :sublinks (nreverse *select-sublinks*) :table table :columns columns :scope scope
                      :source-relations (loop for child in (list source dml-source)
                                              when child collect (list :subquery-prepared child))
                      :relations nil :where where
                      :targets (append (nreverse targets) (getf conflict :targets) returning)
                      :returning returning :conflict conflict
                      :rows rows :sets (sort sets #'< :key (lambda (item) (pg-column-attnum (car item)))) :attnums attnums
                      :insert-source source :insert-source-plan nil :source-assignments source-assignments
                      :dml-source dml-source :dml-source-plan nil :dml-one-time-quals nil :dml-row-where nil))))))
    (clear-query-analysis-contexts prepared)
    (setf prepared (prepare-dml-column-defaults prepared ast))
    (plan-query-ctes prepared)
    (when (getf prepared :insert-source)
      (setf (getf prepared :insert-source-plan)
            (plan-prepared-select (getf prepared :insert-source))))
    (when (getf prepared :dml-source)
      (setf (getf prepared :dml-source-plan) (plan-prepared-select (getf prepared :dml-source))))
    (plan-expression-subqueries prepared)
    (prepare-joined-dml-qualifications prepared)))

(defun revalidate-subquery-relations (relations)
  (dolist (relation relations)
    (unless (getf relation :virtual-p)
      (let* ((table (getf relation :table))
             (path (getf relation :path))
             (index (and (eq (select-scan-path-kind path) :index-scan)
                         (select-scan-path-index path))))
        (ensure-current-table table)
        (unless (eq (getf relation :columns) (pg-class-columns table))
          (ereport "0A000" "table definition changed during query planning"))
        (when (and index (not (select-index-current-p table index)))
          (setf (getf relation :path) (make-select-scan-path :kind :seq-scan))))))
  relations)

(defun call-with-dml-subqueries (prepared function)
  ;; Admit the target write and every source read together. Registering the target
  ;; first can deadlock with an index builder waiting on that writer while blocking
  ;; a source reader. Admission also excludes target index DDL during revalidation.
  (let* ((table (getf prepared :table))
         (relfilenode (pg-class-relfilenode table))
         (relations (mapcar #'copy-list
                            (append (when (getf prepared :insert-source-plan)
                                      (getf (query-plan-prepared (getf prepared :insert-source-plan)) :relations))
                                    (when (getf prepared :dml-source-plan)
                                      (getf (query-plan-prepared (getf prepared :dml-source-plan)) :relations))
                                    (expression-subquery-relations prepared)
                                    (query-cte-relations prepared))))
         (xid (get-current-xid))
         (snapshot *current-snapshot*))
    (register-relation-accesses (list relfilenode)
                                (select-relation-reader-relfilenodes relations) xid)
    (with-write-lock
      (ensure-current-table table)
      (unless (eq (getf prepared :columns) (pg-class-columns table))
        (ereport "0A000" "table definition changed during query planning"))
      (refresh-on-conflict-arbiters table (getf prepared :conflict))
      (revalidate-subquery-relations relations))
    (let* ((*dml-column-defaults* (getf prepared :column-defaults))
           (*subquery-execution-active-p* t)
           (*subquery-result-cache* (make-hash-table :test #'eq))
           (*subquery-registered-relations* relations)
           (*subquery-outer-frames* nil)
           (*subquery-backend-entry* (query-plan-backend-entry :interpreter))
           (*current-snapshot* snapshot)
           (domain (query-plan-index-relfilenodes
                    (%make-query-plan :physical '(:result)) relations)))
      (call-with-query-cte-execution
       t
       (lambda ()
         (call-with-select-index-lease-domain
          domain
          (lambda ()
            ;; SQL expressions and target writes must not retain B-tree leases.
            (detach-select-index-scans)
            (call-with-cte-definitions (getf prepared :ctes)
                                       (lambda ()
                                         (let ((*dml-returning-spool* (when (getf prepared :returning) (make-query-row-spool))))
                                           (unwind-protect (funcall function table relfilenode xid)
                                             (when *dml-returning-spool* (close-query-row-spool *dml-returning-spool*)))))))))))))

(defun call-with-dml-expression-evaluation (function)
  ;; Release enclosing leases before evaluation; child plans acquire their own
  ;; groups. Discharge remaining leases on exit before returning to mutation.
  (detach-select-index-scans)
  (unwind-protect (funcall function)
    (detach-select-index-scans)))

(defun coerce-dml-column-default (column)
  (cond ((and *dml-column-defaults* (gethash column *dml-column-defaults*))
         (eval-expr (gethash column *dml-column-defaults*) #()))
        ((pg-column-default-expression column) (insert-column-default column))
        (t (let ((value (insert-column-default column)))
             (if (eq value +null+) value
                 (eval-subquery-assignment
                  (analyze-subquery-assignment (list :const value :type :int8 :typmod -1) column) #()))))))

(defun subquery-update-values (table sets old &optional (environment old))
  ;; Analysis orders SET assignments by physical attribute; evaluate each against
  ;; the same old row, including volatile expressions and defaults.
  (let* ((columns (pg-class-columns table))
         (new (copy-seq old)))
    (dolist (assignment sets)
      (let* ((column (car assignment))
             (expression (cdr assignment))
             (value (if (eq expression :default) (coerce-dml-column-default column)
                        (eval-expr expression environment))))
        (setf (aref new (1- (pg-column-attnum column)))
              (coerce-value value (pg-column-type column) (pg-column-typmod column)))))
    (check-not-null new columns)
    (check-table-constraints table new)
    new))

(defun exec-insert-subqueries (stream ast)
  (let ((prepared (prepare-dml-subqueries ast)))
    (call-with-dml-subqueries
     prepared
     (lambda (table rel xid)
       (declare (ignore rel xid))
       (let ((columns (getf prepared :columns)) (count 0)
                                                (state (make-hash-table :test #'equal))
                                                (returning (getf prepared :returning)))
         (send-dml-returning-description stream returning)
         (labels
             ((insert-row (expressions environment)
                (check-for-interrupts)
                (let ((values
                        (call-with-dml-expression-evaluation
                         (lambda ()
                           (let ((values (make-array (length columns) :initial-element +null+))
                                 ;; Multi-row VALUES evaluates supplied expressions in source order
                                 ;; before omitted defaults in physical attribute order.
                                 (source-values
                                   (when (cdr (getf prepared :rows))
                                     (coerce
                                      (loop for expression in expressions
                                            for attnum in (getf prepared :attnums)
                                            collect (if (eq expression :default)
                                                        (coerce-dml-column-default
                                                         (aref columns (1- attnum)))
                                                        (eval-expr expression environment)))
                                      'vector))))
                             (loop for column across columns for attnum from 1
                                   for source-position = (position attnum (getf prepared :attnums))
                                   for expression = (when source-position
                                                      (nth source-position expressions))
                                   do (setf (aref values (1- attnum))
                                            (cond ((and source-values source-position)
                                                   (aref source-values source-position))
                                                  ((or (null source-position) (eq expression :default))
                                                   (coerce-dml-column-default column))
                                                  (t (eval-expr expression environment)))))
                             (check-not-null values columns)
                             (check-table-constraints table values)
                             values)))))
                  (detach-select-index-scans)
                  (multiple-value-bind (action actual old)
                      (if (getf prepared :conflict)
                          (insert-tuple-on-conflict table values (getf prepared :conflict) state)
                          (multiple-value-bind (block off publications)
                              (insert-tuple-with-unique table values :publication-kind :insert)
                            (declare (ignore block off))
                            (logical-note-row-change :insert table :new-values values
                                                     :publication-selection publications)
                            (values :insert values nil)))
                    (unless (eq action :nothing)
                      (incf count)
                      (emit-dml-returning stream returning actual :old-values old :new-values actual))))))
           ;; Stream the independent SELECT projection under the original snapshot,
           ;; including when source and target are the same relation.
           (let ((source (getf prepared :insert-source-plan)))
             (if source
                 (execute-query-plan
                  source (subquery-registered-relations source)
                  (lambda (row)
                    (insert-row (mapcar (lambda (target) (getf target :expr))
                                        (getf prepared :source-assignments))
                                (car row))))
                 (dolist (row (getf prepared :rows)) (insert-row row nil)))))
         (finish-dml-command stream returning (format nil "INSERT 0 ~a" count)))))))

(defun exec-delete-subqueries (stream ast)
  (let* ((prepared (prepare-dml-subqueries ast))
         (where (or (getf prepared :dml-row-where) (getf prepared :where))))
    (call-with-dml-subqueries
     prepared
     (lambda (table rel xid)
       (let ((columns (getf prepared :columns)) (count 0))
         (send-dml-returning-description stream (getf prepared :returning))
         (stream-joined-dml-candidates
          prepared table
          (lambda (candidate source environment modified-p)
            (declare (ignore environment))
            (check-for-interrupts)
            (detach-select-index-scans)
            (unless modified-p
              (multiple-value-bind (deleted tuple block off status publications)
                  (heapam-delete-tid
                   rel columns (htup-block candidate) (htup-offnum candidate) xid
                   (backend-command-id *backend*)
                   ;; Recheck the latest target with the original matched source, like
                   ;; EPQ's nonlocking source rowmarks.
                   (lambda (tuple)
                     (call-with-dml-expression-evaluation
                      (lambda ()
                        (or (null where)
                            (true-p (eval-expr where (concatenate 'vector (htup-values tuple) source)))))))
                   (lambda () (logical-capture-publications (list table) :delete))
                   t candidate)
                (declare (ignore block off))
                (check-heapam-write-status status)
                (when deleted
                  (logical-note-row-change :delete table :old-values (htup-values tuple)
                                           :publication-selection publications)
                  (emit-dml-returning stream (getf prepared :returning) (htup-values tuple)
                                      :old-values (htup-values tuple) :new-values nil :source-values source)
                  (incf count))
                deleted))))
         (finish-dml-command stream (getf prepared :returning) (format nil "DELETE ~a" count)))))))

(defun exec-update-subqueries (stream ast)
  (let* ((prepared (prepare-dml-subqueries ast))
         (where (or (getf prepared :dml-row-where) (getf prepared :where)))
         (sets (getf prepared :sets)))
    (call-with-dml-subqueries
     prepared
     (lambda (table rel xid)
       (let ((columns (getf prepared :columns)) (count 0))
         (send-dml-returning-description stream (getf prepared :returning))
         (stream-joined-dml-candidates
          prepared table
          (lambda (candidate source environment modified-p)
            (check-for-interrupts)
            ;; Project every qualifying match before suppressing duplicate mutations,
            ;; as C evaluates the plan output before TM_SelfModified handling.
            (let ((new (call-with-dml-expression-evaluation
                        (lambda () (subquery-update-values table sets (htup-values candidate) environment)))))
              (unless modified-p
                (multiple-value-bind (deleted tuple old-block old-offnum status)
                    (heapam-delete-tid
                     rel columns (htup-block candidate) (htup-offnum candidate) xid
                     (backend-command-id *backend*)
                     (lambda (tuple)
                       (call-with-dml-expression-evaluation
                        (lambda ()
                          ;; EPQ replaces only the target version; the matched source values
                          ;; remain those captured by the original candidate.
                          (let ((environment (concatenate 'vector (htup-values tuple) source)))
                            (when (or (null where) (true-p (eval-expr where environment)))
                              (setf new (subquery-update-values table sets (htup-values tuple) environment))
                              t)))))
                     nil t candidate)
                  (check-heapam-write-status status)
                  (when deleted
                    (multiple-value-bind (block off publications)
                        (insert-tuple-with-unique
                         table new :exclude-tid (cons old-block old-offnum)
                         :publication-kind :update
                         :on-success
                         (lambda (block off)
                           (unless (heapam-set-ctid-unlocked rel old-block old-offnum block off)
                             (ereport "XX000" "could not link updated heap tuple"))))
                      (declare (ignore block off))
                      (logical-note-row-change :update table :old-values (htup-values tuple)
                                               :new-values new :publication-selection publications)
                      (emit-dml-returning stream (getf prepared :returning) new
                                          :old-values (htup-values tuple) :new-values new :source-values source))
                    (incf count))
                  deleted)))))
         (finish-dml-command stream (getf prepared :returning) (format nil "UPDATE ~a" count)))))))
