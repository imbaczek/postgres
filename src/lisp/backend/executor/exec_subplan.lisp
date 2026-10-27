;;;; exec_subplan.lisp -- planned expression subqueries within one statement.
(in-package #:pg)

(defun simplify-exists-prepared-query (prepared)
  ;; Drop output work only when it cannot alter whether EXISTS returns a row.
  (when (and (not (getf prepared :set-operation))
             (zerop (getf prepared :aggregate-count 0))
             (not (getf prepared :set-functions))
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

(defun subquery-cache-for (descriptor)
  "Return the live dependency owner's cache, or NIL for a directly correlated scan."
  (when (member (sql-subplan-kind descriptor) '(:scalar :exists :array))
    (let ((depths (sql-subplan-depths descriptor)))
      (cond ((null depths) *subquery-result-cache*)
            ((member 1 depths) nil)
            (t (cdr (nth (- (first depths) 2) *subquery-outer-frames*)))))))

(defun subquery-quantified-step (kind accumulated value)
  (ecase kind
    (:any (sql-3val-or accumulated value))
    (:all (sql-3val-and accumulated value))))

(defun eval-subquery-expression (descriptor environment)
  ;; Cache only at the dependency owner; correlated scans use copied outer frames.
  (let* ((cache (subquery-cache-for descriptor))
         (key (and cache descriptor)))
    (when key
      (multiple-value-bind (value found) (gethash key cache)
        (when found (return-from eval-subquery-expression value))))
    (let* ((kind (sql-subplan-kind descriptor))
           (plan (sql-subplan-plan descriptor))
           (registered (subquery-registered-relations plan))
           (caller-frames *subquery-outer-frames*)
           (found nil)
           (result (case kind (:all t) ((:scalar) +null+) (t nil)))
           (array (when (eq kind :array) (make-array 0 :adjustable t :fill-pointer 0))))
      (reconcile-query-plan-access-paths plan registered)
      (let ((*subquery-outer-frames*
              (cons (cons (map 'vector #'copy-subquery-value environment) (make-hash-table :test #'eq))
                    caller-frames)))
        (execute-query-plan
         plan registered
         (lambda (row)
           (check-for-interrupts)
           (case kind
             (:exists (setf result t) :stop)
             (:scalar
              (when found
                (ereport "21000" "more than one row returned by a subquery used as an expression"))
              (setf found t result (copy-subquery-value (aref (car row) 0)))
              nil)
             (:array
              (vector-push-extend (copy-subquery-value (aref (car row) 0)) array)
              nil)
             ((:any :all)
              (let* ((*subquery-outer-frames* caller-frames)
                     (*subquery-output-value* (aref (car row) 0))
                     (value (eval-expr (sql-subplan-test descriptor) environment)))
                (setf result (subquery-quantified-step kind result value))
                (when (if (eq kind :any) (eq result t) (null result)) :stop)))))))
      (when array (setf result (coerce array 'simple-vector)))
      (when key (setf (gethash key cache) result))
      result)))
