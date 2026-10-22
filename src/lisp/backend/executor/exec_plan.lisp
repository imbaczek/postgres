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

(defun delete-query-temp-file (pathname)
  (multiple-value-bind (deleted-p condition)
      (ignore-errors (unlink-query-temp-file pathname))
    (declare (ignore condition))
    (when deleted-p
      (setf *query-temp-files*
            (delete pathname *query-temp-files* :test #'equal))
      (forget-query-temp-file-space pathname)))
  (values))

(defun merge-query-sort-runs (left-path right-path order)
  (let ((output-path nil))
    (with-query-temp-output (pathname output)
      (setf output-path pathname)
      (progn
        (with-open-file (left-input left-path
                                    :direction :input
                                    :external-format :utf-8)
          (with-open-file (right-input right-path
                                       :direction :input
                                       :external-format :utf-8)
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

(defun stream-select-sort (streamer order callback)
  "Sort a stream with bounded in-memory runs and stable external merges."
  (call-with-query-sort-arena
   (lambda (arena)
     (stream-select-sort-with-arena streamer order callback arena))))

(defun stream-select-sort-with-arena (streamer order callback arena)
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
                                                   (lambda (entry) (eval-expr (car entry) (cdr row)))
                                                   order)
                                              row)))
                                    (emit-run (pathname)
                                      (with-open-file (stream pathname :direction :input
                                                                       :external-format :utf-8)
                                        (loop for item = (query-spill-read-record stream)
                                              until (eq item +query-spill-eof+)
                                              do (check-for-interrupts)
                                                 (when (eq (funcall callback (fourth item)) :stop)
                                                   (return-from stream-select-sort-with-arena :stop))))))
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
                                            (when (eq (funcall callback
                                                               (if arena
                                                                   (copy-query-sort-value (fourth item))
                                                                   (fourth item)))
                                                      :stop)
                                              (return-from stream-select-sort-with-arena :stop))))
                                        (progn
                                          (flush-run)
                                          (setf runs (nreverse runs))
                                          (loop while (cdr runs)
                                                do (setf runs (merge-query-sort-run-pass runs order)))
                                          (emit-run (first runs)))))
                               ;; This captured cell lives on the heap. Clear it before arena reset
                               ;; or destruction, including cancellation and downstream errors.
                               (setf buffer nil))))))

(defun interpret-query-plan (plan registered-relations row-callback)
  "Reference interpreter for physical S-expression plans."
  (let ((metadata-map (query-plan-metadata plan)))
    (when *query-plan-instrumentation-p*
      (maphash (lambda (node metadata)
                 (declare (ignore node))
                 (setf (plan-node-metadata-actual-rows metadata) nil
                       (plan-node-metadata-actual-row-count metadata) 0
                       (plan-node-metadata-actual-loops metadata) 0
                       (plan-node-metadata-actual-startup-time-ticks metadata) 0
                       (plan-node-metadata-actual-total-time-ticks metadata) 0))
               metadata-map))
    (labels
        ((run-node (node callback)
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
                      (run-child (child child-callback)
                        (let* ((child-metadata (gethash child metadata-map))
                               (old-total-time
                                 (if child-metadata
                                     (plan-node-metadata-actual-total-time-ticks
                                      child-metadata)
                                     0)))
                          (unwind-protect
                               (run-node child child-callback)
                            (when (and started child-metadata)
                              (incf child-total-time
                                    (- (plan-node-metadata-actual-total-time-ticks
                                        child-metadata)
                                       old-total-time)))))))
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
                         (run-child
                          (third node)
                          (lambda (environment)
                            (when (true-p (eval-expr qual environment))
                              (emit environment))))))
                      (:nested-loop-join
                       (let* ((join (second node))
                              (left (third node))
                              (right (fourth node)))
                         (stream-select-join-environments
                          (lambda (left-callback)
                            (run-child left left-callback))
                          (lambda (right-callback)
                            (run-child right right-callback))
                          join #'emit)))
                      (:project
                       (let ((targets (second node)))
                         (run-child
                          (third node)
                          (lambda (environment)
                            (let ((row (make-select-result-row
                                        environment nil targets)))
                              (when row (emit row)))))))
                      (:sort
                       (when (eq (stream-select-sort
                                  (lambda (sort-callback)
                                    (run-child (third node) sort-callback))
                                  (second node) #'emit)
                                 :stop)
                         (return-from run-node :stop)))
                      (:top-n
                       (let* ((order (second node))
                              (limit (third node))
                              (input (fourth node))
                              (rows
                                (collect-select-top-n
                                 (lambda (callback)
                                   (run-child input callback))
                                 order limit)))
                         (dolist (row rows)
                           (when (eq (emit row) :stop)
                             (return-from run-node :stop)))))
                      (:limit
                       (let ((limit (second node))
                             (count 0))
                         (unless (zerop limit)
                           (run-child
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
                           (run-child
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
                 (when (and started metadata)
                   (incf (plan-node-metadata-actual-loops metadata))
                   (incf (plan-node-metadata-actual-row-count metadata) emitted)
                   (setf (plan-node-metadata-actual-rows metadata)
                         (/ (plan-node-metadata-actual-row-count metadata)
                            (plan-node-metadata-actual-loops metadata)))
                   (let* ((elapsed (- (get-internal-real-time) started))
                          (self-time
                            (max 0 (- elapsed callback-time child-total-time))))
                     (incf (plan-node-metadata-actual-startup-time-ticks metadata)
                           (or startup-time elapsed))
                     (incf (plan-node-metadata-actual-total-time-ticks metadata)
                           (+ self-time child-total-time)))))))))
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

(defun plan-node-average-time-ms (ticks loops)
  (internal-time-ticks->milliseconds (/ ticks loops)))

(defun format-query-plan (plan &optional registered-relations analyze-p
                                 execution-time)
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
        (when analyze-p
          (terpri stream)
          (format stream "Execution Time: ~,3f ms" (or execution-time 0.0d0)))))))

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
