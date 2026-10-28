;;;; exec_subplan.lisp -- planned expression subqueries within one statement.
(in-package #:pg)

(defun simplify-exists-prepared-query (prepared)
  ;; Drop output work only when it cannot alter whether EXISTS returns a row.
  ;; HAVING can reject even the implicit global group, so preserve its plan.
  (when (and (not (getf prepared :set-operation))
             (not (getf prepared :ctes))
             (not (getf prepared :has-having))
             (zerop (getf prepared :aggregate-count 0))
             (not (getf prepared :set-functions))
             (not (getf prepared :windows))
             (or (null (getf prepared :limit)) (plusp (getf prepared :limit))))
    (setf (getf prepared :targets) nil
          (getf prepared :order) nil
          (getf prepared :distinct) nil
          (getf prepared :group-keys) nil
          (getf prepared :aggregate-p) nil
          (getf prepared :limit) nil))
  prepared)

(defun plan-expression-subqueries (prepared)
  (let ((descriptors nil))
    (map-prepared-query-expressions
     (lambda (form level)
       (when (and (zerop level) (subquery-expression-p form))
         (pushnew (second form) descriptors :test #'eq))
       form)
     prepared)
    (setf (getf prepared :sublinks) (nreverse descriptors)))
  (dolist (descriptor (getf prepared :sublinks))
    (let ((child (sql-subplan-prepared descriptor)))
      (when (eq (sql-subplan-kind descriptor) :exists)
        (simplify-exists-prepared-query child))
      (setf (sql-subplan-plan descriptor) (plan-prepared-select child)
            (sql-subplan-depths descriptor) (prepared-query-outer-depths child))))
  prepared)

(defun expression-subquery-relations (prepared)
  (mapcan (lambda (descriptor)
            (copy-list (getf (query-plan-prepared (sql-subplan-plan descriptor)) :relations)))
          (getf prepared :sublinks)))

(defun query-plan-expression-subplans (plan)
  (let ((descriptors nil))
    (labels ((visit (form)
               (cond
                 ((sql-subplan-p form)
                  (unless (member form descriptors :test #'eq)
                    (push form descriptors)
                    (visit (sql-subplan-test form))))
                 ((consp form) (visit (car form)) (visit (cdr form))))))
      (visit (query-plan-physical plan)))
    (nreverse descriptors)))

(defun reset-query-plan-tree-instrumentation (plan &optional (seen (make-hash-table :test #'eq)))
  (unless (gethash plan seen)
    (setf (gethash plan seen) t)
    (reset-query-plan-instrumentation (query-plan-metadata plan))
    (dolist (reference (query-plan-cte-reference-plans plan))
      (reset-query-plan-tree-instrumentation reference seen))
    (dolist (definition (query-plan-cte-definitions plan))
      (reset-query-plan-tree-instrumentation (sql-cte-plan definition) seen))
    (dolist (descriptor (query-plan-expression-subplans plan))
      (reset-query-plan-tree-instrumentation (sql-subplan-plan descriptor) seen))))

(defun subquery-registered-relations (plan)
  (mapcar
   (lambda (relation)
     (or (find (getf relation :registration-id) *subquery-registered-relations*
               :key (lambda (candidate) (getf candidate :registration-id)) :test #'eq)
         (error "Subquery relation was not registered by its statement")))
   (getf (query-plan-prepared plan) :relations)))

(defun copy-subquery-value (value)
  "Keep the complete SQL value, including composite field metadata."
  (copy-query-sort-value value))

;; Scalar, EXISTS and ARRAY results can be cached at the frame owning their
;; dependencies.  A depth-one dependency has no reusable owner cache here;
;; worktable-dependent results are additionally scoped to its generation.
(defun subquery-cache-for (descriptor)
  "Return the dependency owner's cache, additionally scoped by worktable generation."
  (when (member (sql-subplan-kind descriptor) '(:scalar :exists :array))
    (let* ((depths (sql-subplan-depths descriptor))
           (owner (cond ((null depths) *subquery-result-cache*)
                        ((member 1 depths) nil)
                        (t (cdr (nth (- (first depths) 2) *subquery-outer-frames*)))))
           (generation (prepared-query-worktable-cache (sql-subplan-prepared descriptor))))
      (if (and owner generation) (recursive-cte-parameter-cache owner generation) owner))))

(defun subquery-quantified-step (kind accumulated value)
  (ecase kind
    (:any (sql-3val-or accumulated value))
    (:all (sql-3val-and accumulated value))))

(defun eval-subquery-expression (descriptor environment)
  (let* ((cache (subquery-cache-for descriptor))
         (key (and cache descriptor)))
    (when key
      (multiple-value-bind (value found) (gethash key cache)
        (when found (return-from eval-subquery-expression value))))
    (let* ((kind (sql-subplan-kind descriptor))
           (plan (sql-subplan-plan descriptor))
           (registered (subquery-registered-relations plan))
           (caller-frames *subquery-outer-frames*)
           (frame (cons (map 'vector #'copy-subquery-value environment) (make-hash-table :test #'eq)))
           (found nil)
           (result (case kind (:all t) (:scalar +null+) (t nil)))
           (array (when (eq kind :array) (make-array 0 :adjustable t :fill-pointer 0))))
      (reconcile-query-plan-access-paths plan registered)
      (let ((*subquery-outer-frames* (cons frame caller-frames)))
        (unwind-protect
             (execute-query-plan
              plan registered
              (lambda (row)
                (check-for-interrupts)
                (case kind
                  (:exists (setf result t) :stop)
                  (:scalar
                   (when found (ereport "21000" "more than one row returned by a subquery used as an expression"))
                   (setf found t result (copy-subquery-value (aref (car row) 0)))
                   nil)
                  (:array (vector-push-extend (copy-subquery-value (aref (car row) 0)) array) nil)
                  ((:any :all)
                   (let* ((*subquery-outer-frames* caller-frames)
                          (*subquery-output-value* (aref (car row) 0))
                          (value (eval-expr (sql-subplan-test descriptor) environment)))
                     (setf result (subquery-quantified-step kind result value))
                     (when (if (eq kind :any) (eq result t) (null result)) :stop))))))
          (close-cte-frame-cache (cdr frame))))
      (when array
        (let ((elements (coerce array 'simple-vector)))
          (setf result (make-sql-array
                        :element-type-id (getf (first (getf (sql-subplan-prepared descriptor) :targets)) :type)
                        :elements elements
                        :dimensions (and (plusp (length elements)) (list (length elements)))
                        :lower-bounds (and (plusp (length elements)) (list 1))))))
      (when key (setf (gethash key cache) result))
      result)))
