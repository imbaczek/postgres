;;;; window.lisp -- syntax and query-owned window expressions.
(in-package #:pg)

(declaim (special *select-set-functions-allowed-p*))

(defvar *select-window-functions* nil)

(defvar *select-window-specifications* nil)

(defvar *window-analysis-targets* nil)

(defvar *window-key-targets* nil)

(defvar *select-named-windows* nil)
(defvar *inside-window-call-p* nil)

(defun parse-window-bound (p)
  (cond ((match-kw p "unbounded")
         (cond ((match-kw p "preceding") :unbounded-preceding)
               ((match-kw p "following") :unbounded-following)
               (t (syntax-error p))))
        ((match-kw p "current") (expect-kw p "row") :current)
        (t (ereport "0A000" "window frame offsets are not supported"))))

(defun parse-window-definition (p)
  (with-parens (p)
    (let ((reference nil) (partition nil) (order nil) (frame nil))
      (when (and (eq (pkind p) :ident)
                 (or (token-quoted-p (pcur p))
                     (not (member (pval p) '("partition" "order" "rows" "range" "groups")
                                  :test #'string=))))
        (setf reference (pident p)))
      (when (match-kw p "partition")
        (expect-kw p "by") (setf partition (parse-expr-list p)))
      (when (match-kw p "order")
        (expect-kw p "by") (setf order (parse-order-list p)))
      (when (or (kw-p p "rows") (kw-p p "range"))
        (let* ((mode (if (match-kw p "rows") :rows
                         (progn (expect-kw p "range") :range)))
               (between (match-kw p "between"))
               (start (parse-window-bound p))
               (end (if between
                        (progn (expect-kw p "and") (parse-window-bound p))
                        :current)))
          (unless (eq start :unbounded-preceding)
            (ereport "0A000" "only UNBOUNDED PRECEDING window starts are supported"))
          (when (eq end :unbounded-preceding)
            (ereport "42P20" "frame end cannot be UNBOUNDED PRECEDING"))
          (setf frame (list mode end))))
      (when (or (kw-p p "groups") (kw-p p "exclude"))
        (ereport "0A000" "window GROUPS and exclusions are not supported"))
      (list :reference reference :partition partition :order order :frame frame))))

(defun parse-window-clause (p)
  (loop collect (let ((name (pident p)))
                  (expect-kw p "as")
                  (cons name (parse-window-definition p)))
        while (match-punct p #\,)))

(defun resolve-window-definition (raw &optional (definitions *select-named-windows*))
  (let* ((direct (stringp raw))
         (name (if direct raw (getf raw :reference)))
         (parent (when name (cdr (assoc name definitions :test #'string=)))))
    (when (and name (null parent))
      (ereport "42704" "window \"~a\" does not exist" name))
    (when direct (return-from resolve-window-definition parent))
    (when parent
      (when (getf raw :partition)
        (ereport "42P20" "cannot override PARTITION BY clause"))
      (when (and (getf parent :order) (getf raw :order))
        (ereport "42P20" "cannot override ORDER BY clause"))
      (when (getf parent :frame)
        (ereport "42P20" "cannot copy window with a frame clause")))
    (list :partition (or (getf raw :partition) (getf parent :partition))
          :order (or (getf raw :order) (getf parent :order))
          :frame (getf raw :frame))))

(defun prepare-named-windows (definitions)
  (let ((result nil))
    (dolist (definition definitions result)
      (when (assoc (car definition) result :test #'string=)
        (ereport "42P20" "window \"~a\" is already defined" (car definition)))
      (push (cons (car definition)
                  (resolve-window-definition (cdr definition) result))
            result))))

(defun window-constant-expression-p (expression)
  (and (consp expression)
       (case (first expression)
         ((:const :bool) t)
         (:binop (and (window-constant-expression-p (third expression))
                      (window-constant-expression-p (fourth expression))))
         (:unary (window-constant-expression-p (third expression)))
         (:cast (window-constant-expression-p (second expression)))
         (otherwise nil))))

(defun fold-window-key-constant (expression)
  (if (and (not *cte-definition-analysis-p*) (window-constant-expression-p expression))
      (list :const (eval-expr expression #()) :type (expr-type expression)
            :typmod (expr-typmod expression))
      expression))

(defun analyze-window-specification (raw scope)
  ;; Resolve keys against selected and earlier key targets after target analysis.
  ;; SQL99 matching may reuse a target; constant folding is limited to key forms.
  (or (find raw *select-window-specifications* :key (lambda (spec) (getf spec :raw)) :test #'equal)
      (let* ((*inside-window-call-p* t)
             (*select-set-functions-allowed-p* nil))
        (labels ((resolve-key (form)
                   (let ((expression
                           (analyze-order-expression form scope
                                                     (append *window-analysis-targets* *window-key-targets*))))
                     (unless (find expression *window-key-targets*
                                   :key (lambda (target) (getf target :expr)) :test #'eq)
                       (setf *window-key-targets*
                             (append *window-key-targets* (list (list :expr expression)))))
                     (fold-window-key-constant expression))))
          (let ((spec (list :raw raw
                            :partition (mapcar #'resolve-key (getf raw :partition))
                            :order (mapcar (lambda (item) (cons (resolve-key (getf item :expr))
                                                                (getf item :desc)))
                                           (getf raw :order))
                            :frame (or (getf raw :frame) '(:range :current)))))
            (vector-push-extend spec *select-window-specifications*)
            spec)))))

(defun analyze-window-call (expression scope)
  ;; Window calls belong to their syntactic query; volatile inputs cannot share
  ;; an executable function slot even when specifications can share a stage.
  (when (and (not *inside-window-call-p*) (getf *select-analysis-frame* :inside-input))
    (ereport "42803" "aggregate function calls cannot contain window function calls"))
  (unless (and *select-window-functions*
               (member (getf *select-analysis-frame* :kind) '(:select-target :order-by))
               (not *inside-window-call-p*)
               (not (getf *select-analysis-frame* :inside-input)))
    (ereport "42P20" "window functions are not allowed in this expression"))
  (let* ((*inside-window-call-p* t)
         (*select-set-functions-allowed-p* nil)
         (call (second expression))
         (raw (resolve-window-definition (third expression)))
         (name (and (eq (first call) :func) (builtin-function-name (second call) (getf (cdddr call) :schema))))
         (kind (or (raw-aggregate-kind call)
                   (cond ((equal name "row_number") :row-number)
                         ((equal name "rank") :rank)
                         ((equal name "dense_rank") :dense-rank))))
         (input nil) (type :int8))
    (unless kind
      (when (member name '("lead" "lag" "first_value" "last_value" "nth_value"
                           "ntile" "percent_rank" "cume_dist" "count") :test #'equal)
        (ereport "0A000" "unsupported window function"))
      (analyze-expr call scope)
      (ereport "42809" "OVER requires a window or aggregate function"))
    (if (member kind '(:row-number :rank :dense-rank))
        (progn
          (mapc (lambda (arg)
                  (analyze-expr (if (eq (first arg) :named-arg) (third arg) arg) scope))
                (third call))
          (when (third call) (ereport "42883" "window ranking function takes no arguments")))
        (multiple-value-setq (input type) (analyze-aggregate-input call scope kind)))
    (when (and (eq (first call) :func) (getf (cdddr call) :call-distinct))
      (ereport "0A000" "DISTINCT is not implemented for window functions"))
    (when (and (eq (first call) :func) (getf (cdddr call) :call-order))
      (ereport "0A000" "aggregate ORDER BY is not implemented for window functions"))
    (let* ((spec (when *window-analysis-targets* (analyze-window-specification raw scope)))
           (partition (getf spec :partition))
           (order (getf spec :order))
           (frame (getf spec :frame))
           (descriptor (append (list :kind kind :input input :type type :partition partition :order order :frame frame)
                               (unless spec (list :raw-spec raw))))
           (key (subquery-semantic-key descriptor))
           (existing (unless (analyzed-expression-volatile-p input)
                       (position-if (lambda (other)
                                      (analyzed-expression-identical-p descriptor other))
                                    *select-window-functions*)))
           (index (or existing (length *select-window-functions*))))
      (unless existing (vector-push-extend descriptor *select-window-functions*))
      (list :window-ref index :type type :typmod -1
            :window-key (make-sql-expression-key :value key)))))

(defun resolve-query-window-specifications (scope targets)
  ;; Defer specifications until all targets exist, then refresh semantic keys.
  (loop for descriptor across *select-window-functions*
        for raw = (getf descriptor :raw-spec)
        when raw
          do (let ((spec (analyze-window-specification raw scope)))
               (setf (getf descriptor :partition) (getf spec :partition)
                     (getf descriptor :order) (getf spec :order)
                     (getf descriptor :frame) (getf spec :frame))
               (remf descriptor :raw-spec)))
  (dolist (definition (reverse *select-named-windows*))
    (analyze-window-specification (cdr definition) scope))
  (labels ((refresh (expression)
             (cond ((atom expression) expression)
                   ((eq (first expression) :window-ref)
                    (setf (getf (cddr expression) :window-key)
                          (make-sql-expression-key
                           :value (subquery-semantic-key
                                   (aref *select-window-functions* (second expression)))))
                    expression)
                   ((subquery-expression-p expression) expression)
                   (t (mapcar #'refresh expression)))))
    (dolist (target targets)
      (setf (getf target :expr) (refresh (getf target :expr))))))

(defun map-window-expressions (function descriptor)
  (setf (getf descriptor :input) (funcall function (getf descriptor :input))
        (getf descriptor :partition) (mapcar function (getf descriptor :partition))
        (getf descriptor :order) (mapcar (lambda (item) (cons (funcall function (car item)) (cdr item)))
                                         (getf descriptor :order)))
  descriptor)

(defun lower-window-references (expression width)
  (cond ((atom expression) expression)
        ((eq (first expression) :window-ref)
         (append (list :col (+ width 1 (second expression)))
                 (cddr expression) (list :window-output t)))
        ((subquery-expression-p expression) expression)
        (t (mapcar (lambda (part) (lower-window-references part width)) expression))))

(defun finalize-query-windows (targets order group-keys scope aggregate-p width)
  ;; Rewrite grouped inputs first, then assign shared pre-window key slots and
  ;; lower window references after those slots (planner.c / setrefs.c).
  (let ((keys nil) (used nil))
    ;; C's SQL99 target list evaluates selected expressions before independent
    ;; junk keys. Whole selected keys share a slot; arbitrary subexpressions do not.
    (when (and *cte-definition-analysis-p* *window-key-targets*
               (zerop (length *select-window-functions*)))
      (labels ((set-expression-p (form)
                 (and (consp form)
                      (or (member (first form) '(:set-func :single-record))
                          (some #'set-expression-p form)))))
        (dolist (target targets)
          (let ((expression (getf target :expr)))
            (unless (set-expression-p expression)
              (unless (find expression keys :test #'analyzed-expression-identical-p)
                (setf keys (append keys (list expression)))))))))
    (dolist (descriptor (coerce *select-window-functions* 'list))
      (when aggregate-p
        (map-window-expressions
         (lambda (expression) (rewrite-aggregate-columns expression group-keys scope))
         descriptor))
      (dolist (expression (append (getf descriptor :partition)
                                  (mapcar #'car (getf descriptor :order))))
        (unless (find expression keys :test #'analyzed-expression-identical-p)
          (setf keys (append keys (list expression))))))
    ;; Shared CTE plans retain named-window junk targets. Keep their slots
    ;; canonical with selected expressions; inlined plans can null unused slots.
    (when *cte-definition-analysis-p*
      (dolist (target *window-key-targets*)
        (let ((expression (getf target :expr)))
          (when aggregate-p
            (setf expression (rewrite-aggregate-columns expression group-keys scope)))
          (unless (find expression keys :test #'analyzed-expression-identical-p)
            (setf keys (append keys (list expression)))))))
    (labels ((key-reference (expression)
               (let ((position (position expression keys :test #'analyzed-expression-identical-p)))
                 (cond (position
                        (pushnew position used)
                        (list :col (+ width position 1) :type (expr-type expression)
                              :typmod (expr-typmod expression)))
                       ((or (atom expression)
                            (member (first expression) '(:subplan :outer-ref :window-ref)))
                        expression)
                       (t (mapcar #'key-reference expression))))))
      (dolist (descriptor (coerce *select-window-functions* 'list))
        (setf (getf descriptor :input) (key-reference (getf descriptor :input))
              (getf descriptor :partition) (mapcar #'key-reference (getf descriptor :partition))
              (getf descriptor :order)
              (mapcar (lambda (item) (cons (key-reference (car item)) (cdr item)))
                      (getf descriptor :order))))
      (dolist (target targets)
        (setf (getf target :expr)
              (lower-window-references (key-reference (getf target :expr)) (+ width (length keys)))))
      (values
       (mapcar (lambda (item)
                 (cons (lower-window-references (key-reference (car item)) (+ width (length keys)))
                       (cdr item)))
               order)
       (+ width (length keys))
       keys
       (loop for position below (length keys) unless (member position used) collect position)))))
