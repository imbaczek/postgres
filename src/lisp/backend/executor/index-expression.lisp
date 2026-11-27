;;;; Resolve the stored expressions of a B-tree definition.

(in-package #:pg)

(defun immutable-index-cast-p (source target)
  "Classify resolved casts whose result is independent of session settings."
  (or (eq source target)
      (and (member source '(:date :timestamp :time))
           (member target '(:date :timestamp :time)))
      (and (or (eq source :bytea) (eq target :bytea))
           (member source '(:bytea :int2 :int4 :int8))
           (member target '(:bytea :int2 :int4 :int8)))
      (let ((scalar-types '(:bool :int2 :int4 :int8 :float4 :float8 :numeric
                            :text :varchar :name :bit :oid :oid8)))
        (and (member source scalar-types)
             (member target scalar-types)
             ;; Like C float4out/float8out, this conversion observes extra_float_digits.
             (not (and (member source '(:float4 :float8))
                       (member target '(:text :varchar :name))))))))

(defun validate-index-expression (expression context note-column)
  "Verify supported immutable nodes and report referenced table attribute numbers."
  (flet ((visit (node walk)
           (declare (ignore walk))
           (let ((tag (first node)))
             (case tag
               (:col (funcall note-column (second node)))
               ((:const :bool :scalar-array-const :not :is-null :is-bool :and :or
                 :case :array :coalesce :array-length :array-subscript :array-slice
                 :unary :concat :in :join-relabel) nil)
               (:func
                (unless (eq :immutable (scalar-builtin-family-volatility (second node)))
                  (ereport "42P17" "functions in ~a must be marked IMMUTABLE" context)))
               (:cast
                (unless (immutable-index-cast-p (expr-type (second node)) (expr-type node))
                  (ereport "42P17" "cast in ~a must be immutable" context)))
               ((:binop :any :all)
                (let* ((left (expr-type (third node)))
                       (right (expr-type (fourth node)))
                       (right (or (sql-array-element-type right) right)))
                  (when (and (not (eq left right))
                             (or (eq left :timestamptz) (eq right :timestamptz)))
                    (ereport "42P17" "operator in ~a must be immutable" context))))
               (otherwise
                ;; Argument lists in the shared walker have no expression tag.
                (unless (consp tag)
                  (ereport "0A000" "unsupported expression node in ~a" context))))
             nil)))
    (declare (dynamic-extent #'visit))
    (some-local-plan-expression #'visit expression))
  expression)

(defun analyze-index-definition (ast table)
  "Return analyzed keys, an optional Boolean predicate and sorted attribute dependencies.
Column keys remain integers. Expression keys carry their resolved types and typmods."
  (let ((dependencies nil)
        (*select-set-functions-allowed-p* nil))
    (labels ((note-column (attnum) (pushnew attnum dependencies :test #'eql))
             (cook (raw context &optional predicate-p)
               (validate-stored-expression raw context table)
               (let ((expression (analyze-expr raw table)))
                 (validate-index-expression
                  (if predicate-p (coerce-boolean-expression expression context) expression)
                  context #'note-column))))
      (let ((keys (mapcar (lambda (key)
                            (if (stringp key)
                                (let ((column (find-column table key)))
                                  (unless column (ereport "42703" "column ~s does not exist" key))
                                  (let ((attnum (pg-column-attnum column)))
                                    (note-column attnum)
                                    attnum))
                                (let ((expression (cook key "index expression")))
                                  (if (eq (first expression) :col) (second expression) expression))))
                          (or (getf ast :index-keys) (getf ast :columns))))
            (predicate (when (getf ast :predicate)
                         (cook (getf ast :predicate) "index predicate" t))))
        (values keys predicate (sort dependencies #'<))))))

(defun make-analyzed-index-object (oid name table ast)
  "Create detached metadata for an analyzed partial or expression index."
  (multiple-value-bind (keys predicate dependencies) (analyze-index-definition ast table)
    (check-index-key-count keys)
    (dolist (key keys)
      (check-index-key-type (if (integerp key)
                                (pg-column-type (aref (pg-class-columns table) (1- key)))
                                (expr-type key))))
    (let ((expression-keys-p (some #'consp keys)))
      (make-index-object oid name table (unless expression-keys-p keys)
                         :unique (getf ast :unique)
                         :nulls-not-distinct (getf ast :nulls-not-distinct)
                         :expression-keys (when expression-keys-p keys)
                         :predicate predicate
                         :referenced-columns (when (or expression-keys-p predicate) dependencies)))))

(defun index-tuple-qualifies-p (index row)
  "Check the stored predicate against logical values. SQL NULL excludes the row."
  (let ((predicate (pg-index-predicate index)))
    (or (null predicate) (eq t (eval-expr predicate row)))))

(defun evaluate-index-key (index row)
  "Evaluate index keys in definition order from the logical row."
  (let ((keys (pg-index-expression-keys index)))
    (if keys
        (loop for key in keys collect (if (integerp key)
                                          (aref row (1- key))
                                          (eval-expr key row)))
        (index-values index row))))

(defun index-has-expressions-p (index)
  (or (pg-index-predicate index) (pg-index-expression-keys index)))

(defun prepare-expression-index-row-keys (indexes row)
  "Evaluate keys before any heap write and retain them across uniqueness waits."
  (loop for index in indexes
        when (and (index-has-expressions-p index) (index-tuple-qualifies-p index row))
          collect (cons index (evaluate-index-key index row))))

(defun index-row-key-values (index row expression-keys)
  "Return NIL only for an index whose predicate excludes this row."
  (if (index-has-expressions-p index)
      (cdr (assoc index expression-keys :test #'eq))
      (index-values index row)))
