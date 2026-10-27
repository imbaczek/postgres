;;;; exec_cte.lisp -- lexical owners and parameter-epoch shared WITH state.
(in-package #:pg)

(defstruct cte-execution-state definition store cache closed-p)

(defun query-cte-relations (prepared)
  (mapcan (lambda (definition)
            (when (sql-cte-plan definition)
              (copy-list (getf (query-plan-prepared (sql-cte-plan definition)) :relations))))
          (getf prepared :ctes)))

(defun close-cte-execution-state (state)
  (sb-sys:without-interrupts
    (unless (cte-execution-state-closed-p state)
      (setf (cte-execution-state-closed-p state) t)
      (unwind-protect
           (sb-sys:with-local-interrupts
             (when (cte-execution-state-store state)
               (close-query-replay-store (cte-execution-state-store state))))
        (let ((cache (cte-execution-state-cache state))
              (definition (cte-execution-state-definition state)))
          (when (and cache (eq state (gethash definition cache))) (remhash definition cache)))
        (when *cte-open-states* (remhash state *cte-open-states*))
        (setf (cte-execution-state-store state) nil
              (cte-execution-state-cache state) nil
              (cte-execution-state-definition state) nil))))
  (values))

(defun close-cte-frame-cache (cache)
  (when cache
    (let ((states (loop for value being the hash-values of cache
                        when (cte-execution-state-p value) collect value)))
      (dolist (state states)
        (handler-case (close-cte-execution-state state)
          (error () nil) (sb-ext:timeout () nil)))))
  (values))

(defun extend-cte-execution-context (definitions)
  (append (mapcar (lambda (definition) (cons definition *subquery-outer-frames*)) definitions)
          *cte-execution-context*))

(defun call-with-cte-definitions (definitions function)
  (let ((*cte-execution-context* (extend-cte-execution-context definitions)))
    (funcall function)))

(defun make-with-ctes-iterator (definitions node relations)
  (let* ((context (extend-cte-execution-context definitions))
         (input (let ((*cte-execution-context* context))
                  (make-physical-plan-iterator node relations))))
    (make-sql-iterator
     (lambda ()
       (let ((*cte-execution-context* context)) (sql-iterator-next input)))
     (lambda () (close-sql-iterator input) (setf context nil)))))

(defun cte-state-cache (definition frames)
  ;; Correlated producers belong to the live dependency frame, whose cache
  ;; defines their parameter epoch; uncorrelated producers use the root cache.
  (when (sql-cte-shared-p definition)
    (let ((dependencies (sql-cte-dependencies definition)))
      (if (null dependencies) *subquery-result-cache*
          (or (cdr (nth (- (first dependencies) 2) frames))
              (error "CTE producer has no live dependency owner"))))))

(defun plan-cte-reference-filters (prepared)
  ;; Per-reference plans push only safe filters across supported simple shapes;
  ;; they do not flatten the CTE into a joint optimizer search.
  (let ((safe-joins (every (lambda (join) (member (getf join :type) '(:inner :cross)))
                           (getf prepared :joins))))
    (dolist (relation (getf prepared :relations))
      (let* ((definition (getf relation :cte))
             (body (and definition (sql-cte-prepared definition))))
        (when (and definition (not (sql-cte-shared-p definition)))
          (when (and safe-joins
                     (not (getf body :set-operation)) (not (getf body :aggregate-p))
                     (not (getf body :windows)) (not (getf body :window-keys))
                     (not (getf body :set-functions))
                     (not (getf body :distinct)) (null (getf body :limit)))
            (let ((quals (remove-if-not (lambda (form) (cte-reference-filter-p form relation))
                                        (where-conjuncts (getf prepared :where)))))
              (when quals
                (setf (getf relation :cte-filter)
                      (substitute-cte-output-columns
                       (reduce (lambda (left right) (list :and left right :type :bool :typmod -1))
                               (rest quals) :initial-value (first quals))
                       relation)))))
          (let* ((tree (copy-tree (cte-reference-producer-tree
                                   (query-plan-physical (sql-cte-plan definition))
                                   (getf relation :cte-filter))))
                 (plan (%make-query-plan :physical tree :logical tree :prepared body)))
            (dolist (node (collect-query-plan-nodes tree))
              (setf (gethash node (query-plan-metadata plan)) (make-plan-node-metadata)))
            (setf (getf relation :cte-reference-plan) plan))))))
  prepared)

(defun make-cte-producer-state (definition frames context cache &optional reference-plan)
  (let* ((own-frame (cons #() (make-hash-table :test #'eq)))
         (producer-frames (cons own-frame frames))
         (plan (or reference-plan (sql-cte-plan definition)))
         (relations (subquery-registered-relations plan))
         (input
           (let ((*subquery-outer-frames* producer-frames)
                 (*cte-execution-context* context)
                 (*cte-active-plan* plan))
             (make-physical-plan-iterator (query-plan-physical plan) relations)))
         (producer
           (make-sql-iterator
            (lambda ()
              (let ((*subquery-outer-frames* producer-frames)
                    (*cte-execution-context* context)
                    (*cte-active-plan* plan))
                (multiple-value-bind (row present) (sql-iterator-next input)
                  (values (when present (car row)) present))))
            (lambda ()
              (unwind-protect
                   (close-sql-iterator input)
                (unwind-protect
                     (close-cte-frame-cache (cdr own-frame))
                  (setf input nil producer-frames nil own-frame nil context nil relations nil plan nil))))))
         (state (make-cte-execution-state
                 :definition definition :cache cache :store (make-query-replay-store producer))))
    (when cache (setf (gethash definition cache) state))
    (when *cte-open-states* (setf (gethash state *cte-open-states*) t))
    state))

(defun make-cte-scan-iterator (relation)
  ;; Shared readers have independent cursors over a lazy appendable replay store.
  (let* ((definition (getf relation :cte))
         (binding (assoc definition *cte-execution-context* :test #'eq)))
    (unless binding (error "CTE reader has no lexical declaration owner"))
    (let* ((frames (cdr binding))
           (cache (cte-state-cache definition frames))
           (state (or (and cache (gethash definition cache))
                      (make-cte-producer-state definition frames *cte-execution-context* cache (getf relation :cte-reference-plan))))
           (reader (if (sql-cte-shared-p definition)
                       (make-query-replay-reader (cte-execution-state-store state))
                       (query-replay-store-producer (cte-execution-state-store state)))))
      (make-sql-iterator
       (lambda () (sql-iterator-next reader))
       (lambda ()
         (close-sql-iterator reader)
         (unless cache (close-cte-execution-state state))
         (setf reader nil state nil))))))

(defun call-with-query-cte-execution (root-p function)
  (if (not root-p) (funcall function)
      (let ((*cte-execution-context* nil)
            (*cte-open-states* (make-hash-table :test #'eq))
            (*sql-open-iterators* (make-hash-table :test #'eq)))
        (with-query-temp-files ()
                               (unwind-protect (funcall function)
                                 (dolist (iterator (loop for iterator being the hash-keys of *sql-open-iterators*
                                                         collect iterator))
                                   (handler-case (close-sql-iterator iterator)
                                     (error () nil) (sb-ext:timeout () nil)))
                                 (dolist (state (loop for state being the hash-keys of *cte-open-states* collect state))
                                   (handler-case (close-cte-execution-state state)
                                     (error () nil) (sb-ext:timeout () nil))))))))

(defun query-plan-cte-definitions (plan)
  (let ((result nil))
    (dolist (node (collect-query-plan-nodes (query-plan-physical plan)) (nreverse result))
      (when (eq (first node) :with-ctes)
        (dolist (definition (second node))
          (when (sql-cte-plan definition) (pushnew definition result :test #'eq)))))))

(defun query-plan-cte-reference-plans (plan)
  (remove-duplicates
   (loop for relation in (getf (query-plan-prepared plan) :relations)
         when (getf relation :cte-reference-plan) collect (getf relation :cte-reference-plan))
   :test #'eq))
