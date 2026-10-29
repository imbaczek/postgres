;;;; exec_joined_dml.lisp -- source namespaces and bounded replay for mutations.
(in-package #:pg)

(defun prepare-dml-source (ast target-scope)
  ;; Record the source query's lexical level so its LATERAL entries can report
  ;; forbidden target references without exposing that target as a usable input.
  (when ast
    (let ((*dml-source-target-scope* (cons (1+ *select-cte-level*) target-scope)))
      (prepare-select-internal ast nil))))

(defun shift-dml-source-expression (expression offset)
  (cond ((atom expression) expression)
        ((eq (first expression) :col)
         (let ((copy (copy-tree expression))) (incf (second copy) offset) copy))
        (t (mapcar (lambda (part) (shift-dml-source-expression part offset)) expression))))

(defun extend-dml-source-scope (base source offset)
  (if (null source) base
      (let* ((source-scope (getf source :scope))
             (columns (mapcar (lambda (column)
                                (let ((copy (copy-select-column column)))
                                  (setf (select-column-expr copy)
                                        (shift-dml-source-expression (select-column-expr column) offset))
                                  copy))
                              (select-scope-columns source-scope)))
             (result base))
        (dolist (relation (select-scope-relations source-scope))
          (let ((copy (copy-select-relation relation)))
            (incf (select-relation-offset copy) offset)
            (setf result (extend-select-scope result copy))))
        (setf (select-scope-columns result) (append (select-scope-columns base) columns)
              (select-scope-width result) (+ offset (select-scope-width source-scope)))
        result)))

(defun make-dml-source-replay (plan)
  ;; The synthetic SELECT * projection is not the DML source environment:
  ;; JOIN USING can project fewer columns than the raw source slot layout.
  (let* ((relations (subquery-registered-relations plan))
         (frames *subquery-outer-frames*)
         (context *cte-execution-context*)
         (tree (query-plan-physical plan))
         (capture-p (and *datum-origin-capture* (eq (first tree) :project)
                         (simple-datum-origin-plan-p (third tree) relations t)))
         (input (let ((*cte-active-plan* plan) (*datum-origin-capture* capture-p))
                  (reconcile-query-plan-access-paths plan relations)
                  (make-physical-plan-iterator (if capture-p (third tree) tree) relations))))
    (let ((*datum-origin-capture* capture-p))
      (make-query-replay-store
       (make-sql-iterator
        (lambda ()
          (let ((*cte-active-plan* plan) (*subquery-outer-frames* frames)
                                         (*cte-execution-context* context) (*datum-origin-capture* capture-p))
            (multiple-value-bind (row present origins) (sql-iterator-next input)
              (values (when present (if capture-p row (cdr row))) present origins))))
        (lambda () (close-sql-iterator input)))))))

(defun dml-one-time-qualification-p (expression)
  ;; Like is_pseudo_constant_clause, a gate must be row-independent and free of
  ;; volatile calls. Uncorrelated scalar/EXISTS/array subplans can be InitPlans.
  (cond
    ((atom expression) t)
    ((member (first expression) '(:col :outer-ref :subquery-output)) nil)
    ((eq (first expression) :subplan)
     (let ((descriptor (second expression)))
       (and (member (sql-subplan-kind descriptor) '(:scalar :exists :array))
            (null (sql-subplan-depths descriptor)))))
    ((eq (first expression) :func)
     (and (or (having-immutable-function-p (second expression))
              (member (second expression)
                      '("version" "pg_lisp_runtime" "current_database" "current_schema"
                        "current_catalog" "current_user" "session_user" "user"
                        "current_setting" "pg_backend_pid" "pg_postmaster_start_time")
                      :test #'equal))
          (every #'dml-one-time-qualification-p (third expression))))
    (t (every #'dml-one-time-qualification-p expression))))

(defun prepare-joined-dml-qualifications (prepared)
  ;; Run after subplan planning, when correlation depths distinguish one-time
  ;; gates from qualifications that must be evaluated for each candidate.
  (when (getf prepared :dml-source)
    (let ((once nil) (rows nil))
      (dolist (clause (where-conjuncts (getf prepared :where)))
        (if (dml-one-time-qualification-p clause) (push clause once) (push clause rows)))
      (setf (getf prepared :dml-one-time-quals) (nreverse once)
            (getf prepared :dml-row-where)
            (reduce (lambda (left right) (list :and left right :type :bool :typmod -1))
                    (nreverse rows) :initial-value '(:bool t)))))
  prepared)

(defun dml-row-origins (tuple source-origins environment)
  "Combine target and source metadata in the same slots as ENVIRONMENT."
  (let* ((target-origins (htup-origins tuple))
         (target-width (length (htup-values tuple)))
         (result (make-array (length environment) :initial-element nil)))
    (when target-origins
      (replace result target-origins :end1 (min target-width (length target-origins))))
    (when source-origins
      (replace result source-origins :start1 target-width
                                     :end1 (min (length result) (+ target-width (length source-origins)))))
    result))

(defun stream-joined-dml-candidates (prepared table function &optional toast-demand)
  ;; Target tuples carry physical identity; replayed source values are independent
  ;; of that identity and remain from the original statement snapshot. The success
  ;; bit belongs to one target tuple and is set only after an actual mutation, so
  ;; a failed concurrent recheck leaves later source matches eligible.
  (let ((gate (having-input-gate
               (mapcar #'having-gate-skeleton (where-conjuncts (getf prepared :where))))))
    (when (and gate (not (true-p (eval-expr gate #()))))
      (return-from stream-joined-dml-candidates nil)))
  (dolist (qualification (getf prepared :dml-one-time-quals))
    (unless (call-with-dml-expression-evaluation
             (lambda () (true-p (eval-expr qualification #()))))
      (return-from stream-joined-dml-candidates nil)))
  (let* ((plan (getf prepared :dml-source-plan))
         (store (when plan (make-dml-source-replay plan)))
         (where (or (getf prepared :dml-row-where) (getf prepared :where))))
    (unwind-protect
         (do-table-tuples (tuple table)
           (materialize-select-tuple table tuple *current-snapshot*
                                     :demand toast-demand :retain-external-p (not (null toast-demand)))
           (let ((modified-p nil))
             (flet ((offer (source)
                      (let* ((environment (concatenate 'vector (htup-values tuple) source))
                             (*query-row-origins*
                               (and *datum-origin-capture*
                                    (dml-row-origins tuple *query-row-origins* environment))))
                        (when (or (null where)
                                  (call-with-dml-expression-evaluation
                                   (lambda () (true-p (eval-expr where environment)))))
                          (when (funcall function tuple source environment modified-p)
                            (setf modified-p t))))))
               (if store
                   (let ((reader (make-query-replay-reader store)))
                     (unwind-protect (map-sql-iterator reader #'offer)
                       (close-sql-iterator reader)))
                   (offer #())))))
      (when store (close-query-replay-store store)))))
