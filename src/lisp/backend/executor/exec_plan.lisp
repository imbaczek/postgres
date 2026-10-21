;;;; exec_plan.lisp -- S-expression query plans and their reference interpreter.

(in-package #:pg)

(defstruct plan-node-metadata
  estimated-rows
  estimated-cost
  actual-rows
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
(defvar *query-plan-backend-lock*
  (sb-thread:make-mutex :name "pg query-plan backend registry"))

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

(defun execute-query-plan (plan registered-relations row-callback
                           &key (backend :interpreter))
  "Execute PLAN through BACKEND. The interpreter is the reference backend."
  (let* ((entry (query-plan-backend-entry backend))
         (executor (and entry (query-plan-backend-executor entry))))
    (cond
      (executor
       (funcall executor plan registered-relations row-callback))
      ((and entry (query-plan-backend-compiler entry))
       (funcall (funcall (query-plan-backend-compiler entry) plan)
                registered-relations row-callback))
      (t
       (ereport "0A000" "query-plan backend ~a is not installed" backend)))))

(defun query-plan-sexp (plan)
  "Return the inspectable logical/physical plan artifact."
  (list :logical (query-plan-logical plan)
        :physical (query-plan-physical plan)
        :alternatives (query-plan-alternatives plan)))

(defun select-plan-path-candidates (prepared)
  "Build v1 path alternatives: an applicable equality index, then SeqScan."
  (let* ((relations (getf prepared :relations))
         (joins (getf prepared :joins))
         (table (getf prepared :table)))
    (cond
      ((null relations)
       (values nil (list nil)))
      ((and (= (length relations) 1) (null joins))
       (let* ((best (with-write-lock
                      (choose-select-scan-path table (getf prepared :where))))
              (seq (make-select-scan-path :kind :seq-scan)))
         (if (eq (select-scan-path-kind best) :index-scan)
             (values (list best) (list (list best) (list seq)))
             (values (list seq) (list (list seq))))))
      (t
       (let ((seq-paths (loop repeat (length relations)
                              collect (make-select-scan-path
                                       :kind :seq-scan))))
         (values seq-paths (list seq-paths)))))))

(defun select-scan-plan-form (relation-index relation path physical-p)
  (let ((table (getf relation :table)))
    (if (not physical-p)
        (list :scan relation-index (pg-class-oid table))
        (if (eq (select-scan-path-kind path) :index-scan)
            (list :index-scan relation-index (pg-class-oid table)
                  (pg-index-oid (select-scan-path-index path))
                  (select-scan-path-qual path)
                  (select-scan-path-key path))
            (list :seq-scan relation-index (pg-class-oid table))))))

(defun select-plan-input-form (prepared physical-p paths)
  (let* ((relations (getf prepared :relations))
         (joins (getf prepared :joins)))
    (if (null relations)
        '(:result)
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
          input))))

(defun build-select-plan-tree (prepared physical-p paths)
  (let* ((input (select-plan-input-form prepared physical-p paths))
         (where (getf prepared :where))
         (aggregate-p (getf prepared :aggregate-p)))
    (when where
      (setf input (list :filter where input)))
    (when aggregate-p
      (setf input
            (list (if physical-p :hash-aggregate :aggregate)
                  (getf prepared :group-keys)
                  (getf prepared :aggregate-count)
                  input)))
    (setf input (list :project (getf prepared :targets) input))
    (when (getf prepared :order)
      (setf input (list :sort (getf prepared :order) input)))
    (when (getf prepared :limit)
      (setf input
            (if (and physical-p (getf prepared :order))
                (list :top-n (getf prepared :order)
                      (getf prepared :limit) (third input))
                (list :limit (getf prepared :limit) input))))
    input))

(defun query-plan-operator-p (node)
  (and (consp node)
       (member (first node)
               '(:scan :seq-scan :index-scan :result :filter :join
                 :nested-loop-join :project :aggregate :sort :top-n :limit
                 :hash-aggregate)
               :test #'eq)))

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

(defun plan-select-query (ast)
  "Resolve AST, build logical and physical IR, and retain candidate paths."
  (let* ((prepared (prepare-select ast)))
    (multiple-value-bind (selected-paths alternatives)
        (select-plan-path-candidates prepared)
      (setf (getf prepared :relations)
            (loop for relation in (getf prepared :relations)
                  for path in selected-paths
                  collect (let ((copy (copy-list relation)))
                            (setf (getf copy :path) path)
                            copy)))
      (let* ((logical (build-select-plan-tree prepared nil selected-paths))
             (physical (build-select-plan-tree prepared t selected-paths))
             (alternative-plans
               (mapcar (lambda (paths)
                         (build-select-plan-tree prepared t paths))
                       alternatives))
             (plan (%make-query-plan
                    :logical logical
                    :physical physical
                    :alternatives alternative-plans
                    :prepared prepared)))
        (dolist (node (collect-query-plan-nodes physical))
          (let ((metadata (make-plan-node-metadata)))
            (when (member (first node) '(:seq-scan :index-scan) :test #'eq)
              (let* ((relation (nth (second node)
                                    (getf prepared :relations)))
                     (path (getf relation :path)))
                (setf (plan-node-metadata-selection-rule metadata)
                      (if (eq (select-scan-path-kind path) :index-scan)
                          :equality-index
                          :sequential-fallback))))
            (setf (gethash node (query-plan-metadata plan)) metadata)))
        plan))))

(defun sql-group-value-key (value type)
  "Build a SQL-value key; SQL NULLs compare equal for grouping."
  (if (eq value +null+)
      (list :null type)
      (list :value type value)))

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
                              :key (sixth node))))))

(defun interpret-query-plan (plan registered-relations row-callback)
  "Reference interpreter for physical S-expression plans."
  (let ((metadata-map (query-plan-metadata plan)))
    (labels
        ((run-node (node callback)
           (let ((metadata (gethash node metadata-map))
                 (emitted 0))
             (labels ((emit (row)
                        (incf emitted)
                        (funcall callback row)))
               (unwind-protect
                    (case (first node)
                      (:result (emit #()))
                      ((:seq-scan :index-scan)
                       (let* ((relation (physical-scan-relation
                                         node registered-relations))
                              (table (getf relation :table))
                              (path (physical-scan-path node relation)))
                         (scan-select-path
                          table (pg-class-relfilenode table) path
                          *current-snapshot*
                          (lambda (tuple)
                            (check-for-interrupts)
                            (emit (htup-values tuple))))))
                      (:filter
                       (let ((qual (second node)))
                         (run-node
                          (third node)
                          (lambda (environment)
                            (when (true-p (eval-expr qual environment))
                              (emit environment))))))
                      (:nested-loop-join
                       (let* ((join (second node))
                              (left (third node))
                              (right (fourth node))
                              (right-rows '()))
                         (run-node right
                                   (lambda (row) (push row right-rows)))
                         (setf right-rows (nreverse right-rows))
                         (stream-select-join-environments
                          (lambda (left-callback)
                            (run-node left left-callback))
                          right-rows join #'emit)))
                      (:project
                       (let ((targets (second node)))
                         (run-node
                          (third node)
                          (lambda (environment)
                            (let ((row (make-select-result-row
                                        environment nil targets)))
                              (when row (emit row)))))))
                      (:sort
                       (let ((rows '())
                             (order (second node)))
                         (run-node (third node)
                                   (lambda (row) (push row rows)))
                         (dolist (row
                                  (stable-sort
                                   (nreverse rows)
                                   (lambda (a b) (row-less-p a b order))))
                           (when (eq (emit row) :stop)
                             (return-from run-node :stop)))))
                      (:top-n
                       (let* ((order (second node))
                              (limit (third node))
                              (input (fourth node))
                              (rows
                                (collect-select-top-n
                                 (lambda (callback)
                                   (run-node input callback))
                                 order limit)))
                         (dolist (row rows)
                           (when (eq (emit row) :stop)
                             (return-from run-node :stop)))))
                      (:limit
                       (let ((limit (second node))
                             (count 0))
                         (unless (zerop limit)
                           (run-node
                            (third node)
                            (lambda (row)
                              (incf count)
                              (emit row)
                              (when (>= count limit) :stop))))))
                      (:hash-aggregate
                       (let* ((group-keys (second node))
                              (aggregate-count (third node))
                              (groups (make-hash-table :test #'equal))
                              (group-order '()))
                         (labels ((make-state (values)
                                    (let ((state
                                            (cons (coerce values 'vector)
                                                  (make-array
                                                   aggregate-count
                                                   :initial-element 0))))
                                      (push state group-order)
                                      state)))
                           ;; A plain aggregate has one group even on empty input.
                           (unless group-keys
                             (setf (gethash nil groups) (make-state nil)))
                           (run-node
                            (fourth node)
                            (lambda (environment)
                              (check-for-interrupts)
                              (let* ((values
                                       (mapcar
                                        (lambda (expr)
                                          (eval-expr expr environment))
                                        group-keys))
                                     (key
                                       (loop for expr in group-keys
                                             for value in values
                                             collect
                                             (sql-group-value-key
                                              value (expr-type expr))))
                                     (state (gethash key groups)))
                                (unless state
                                  (setf state (make-state values)
                                        (gethash key groups) state))
                                (dotimes (index aggregate-count)
                                  (incf (aref (cdr state) index))))))
                           (dolist (state (nreverse group-order))
                             (when (eq (emit (concatenate 'vector
                                                          (car state)
                                                          (cdr state)))
                                       :stop)
                               (return-from run-node :stop))))))
                      (t
                       (error "Unknown physical query-plan node ~s" node)))
                 (when metadata
                   (setf (plan-node-metadata-actual-rows metadata) emitted)))))))
      (run-node (query-plan-physical plan) row-callback))))

(register-query-plan-backend :interpreter :executor #'interpret-query-plan)

(defun physical-plan-node-children (node)
  (case (first node)
    ((:filter :project :sort :limit) (list (third node)))
    (:top-n (list (fourth node)))
    (:nested-loop-join (list (third node) (fourth node)))
    (:hash-aggregate (list (fourth node)))
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
    (:filter "Filter")
    (:project "Project")
    (:sort "Sort")
    (:top-n (format nil "Top-N Sort (limit ~a)" (third node)))
    (:limit "Limit")
    (:hash-aggregate "HashAggregate")
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

(defun format-query-plan (plan &optional registered-relations)
  "Format the selected physical tree for EXPLAIN."
  (let ((relations (or registered-relations
                       (getf (query-plan-prepared plan) :relations))))
    (with-output-to-string (stream)
      (labels ((print-node (node depth)
                 (format stream "~a~a"
                         (make-string (* depth 2) :initial-element #\Space)
                         (physical-plan-node-label node relations))
                 (dolist (child (physical-plan-node-children node))
                   (terpri stream)
                   (print-node child (1+ depth)))))
        (print-node (query-plan-physical plan) 0)))))

(defun exec-select (stream ast)
  (let* ((plan (plan-select-query ast))
         (prepared (query-plan-prepared plan))
         (relations (getf prepared :relations))
         (targets (getf prepared :targets))
         (row-count 0))
    (labels ((send-row (row)
               (send-select-row stream row targets)
               (incf row-count)))
      (if relations
          (with-registered-select-relations (registered relations)
            (reconcile-query-plan-access-paths plan registered)
            (send-row-description stream
                                  (mapcar #'target->result-column targets))
            (execute-query-plan plan registered #'send-row))
          (progn
            (send-row-description stream
                                  (mapcar #'target->result-column targets))
            (execute-query-plan plan nil #'send-row)))
      (send-command-complete stream (format nil "SELECT ~a" row-count)))))

(defun exec-explain (stream ast)
  (let* ((plan (plan-select-query ast))
         (prepared (query-plan-prepared plan))
         (relations (getf prepared :relations))
         (description nil))
    (if relations
        (with-registered-select-relations (registered relations)
          (reconcile-query-plan-access-paths plan registered)
          (setf description (format-query-plan plan registered)))
        (setf description (format-query-plan plan)))
    (send-row-description
     stream
     (list (make-result-column :name "QUERY PLAN"
                               :type-oid +textoid+
                               :typlen -1
                               :typmod -1)))
    (send-data-row-text stream (list description))
    (send-command-complete stream "EXPLAIN")))
