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
        (t (map-local-expression-operands
            (lambda (part) (shift-dml-source-expression part offset)) expression))))

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
            (multiple-value-bind (row present origins toast presence) (sql-iterator-next input)
              (declare (ignore toast))
              (values (when present (if capture-p row (cdr row))) present origins nil presence))))
        (lambda () (close-sql-iterator input)))))))

(defun dml-one-time-qualification-p (expression)
  "Recognize row-independent gates using C's is_pseudo_constant_clause rule."
  (not
   (some-local-plan-expression
    (lambda (node walk)
      (declare (ignore walk))
      (case (first node)
        ((:col :outer-ref :subquery-output :whole-row) t)
        (:subplan
         (let ((descriptor (second node)))
           (values (or (not (member (sql-subplan-kind descriptor) '(:scalar :exists :array)))
                       (sql-subplan-depths descriptor))
                   t)))
        (:func
         (not (member (scalar-builtin-family-volatility (second node))
                      '(:immutable :stable))))))
    expression)))

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

(defun fixed-heap-equality-filter (column type literal)
  "Match fixed-width payloads whose encoded equality implies SQL equality."
  (let* ((width (sql-expression-type-length type))
         (alignment (column-align column))
         (comparison
           (case type
             (:interval
              (let ((key (sql-interval-comparison-value literal)))
                (lambda (page offset)
                  (= key (sql-interval-comparison-span
                          (heap-read-int4 page (+ offset 12))
                          (heap-read-int4 page (+ offset 8))
                          (heap-read-int8 page offset))))))
             (:timetz
              ;; Equal UTC times require equal zones to compare equal.
              (let ((time (sql-timetz-microseconds literal))
                    (zone (sql-timetz-zone-west literal)))
                (lambda (page offset)
                  (and (= zone (heap-read-int4 page (+ offset 8)))
                       (= time (heap-read-int8 page offset))))))
             (:uuid
              (let ((key (sql-uuid-bytes literal)))
                (lambda (page offset)
                  (zerop (compare-octet-vector-to-span key page offset width)))))
             ((:int2 :int4 :int8 :date :time :timestamp :timestamptz :float4 :float8)
              (let* ((float-p (floatp literal))
                     ;; NaN and zero each have multiple storage encodings.
                     (bits (if float-p
                               (unless (or (sb-ext:float-nan-p literal) (zerop literal))
                                 (sql-float-bits literal type))
                               (ldb (byte (* 8 width) 0) literal)))
                     (reader (ecase width (2 #'page-u16) (4 #'page-u32) (8 #'page-u64))))
                (when bits
                  (lambda (page offset) (= bits (funcall reader page offset)))))))))
    (when comparison
      (lambda (page start limit infomask)
        (let ((offset (single-fixed-heap-tuple-offset page start limit infomask width alignment)))
          (and offset (funcall comparison page offset)))))))

(defun prepared-single-column-heap-filter (table expression)
  "Reject nonmatching single-column equality rows before deforming their values."
  (let ((columns (pg-class-columns table)))
    (when (and (= (length columns) 1) (eq (first expression) :binop)
               (or (eq (first (third expression)) :const)
                   (eq (first (fourth expression)) :const)))
      (multiple-value-bind (attnum literal found-p) (equality-column-constant expression)
        (let* ((column (aref columns 0))
               (type (pg-column-type column)))
          (when (and found-p (= attnum 1) (not (pg-column-dropped-p column))
                     (indexable-equality-value-p literal type))
            (let ((test
                    (case type
                      ((:text :varchar)
                       (let* ((key (string->bytes literal))
                              (matches (lambda (page start end)
                                         (and (= (length key) (- end start))
                                              (zerop (compare-octet-vector-to-span
                                                      key page start (- end start)))))))
                         (lambda (page start limit infomask)
                           (heap-single-inline-varlena-matches-p matches page start limit infomask))))
                      (:numeric
                       (when (and (or (integerp literal) (sql-decimal-p literal))
                                  (<= 0 (sql-numeric-scale literal) 63)
                                  (typep (sql-numeric-coefficient literal) 'fixnum)
                                  (not (zerop (sql-numeric-coefficient literal))))
                         (let* ((key (sql-numeric-to-varlena-payload literal))
                                (matches (lambda (page start end)
                                           (short-numeric-payload-may-match-p key page start end))))
                           (lambda (page start limit infomask)
                             (heap-single-inline-varlena-matches-p matches page start limit infomask)))))
                      (otherwise (fixed-heap-equality-filter column type literal)))))
              (when test
                (lambda (page start limit infomask)
                  (check-for-interrupts)
                  (or (/= 1 (page-u16 page (+ start +heap-tuple-natts-off+)))
                      (funcall test page start limit infomask)))))))))))

(defun prepared-row-comparison (expression)
  "Prepare a direct column-to-literal comparison for a scan qualification."
  (when (and (eq (first expression) :binop)
             (or (eq (first (third expression)) :const)
                 (eq (first (fourth expression)) :const)))
    (multiple-value-bind (attnum operator literal)
        (index-comparison-constant expression)
      (when attnum
        (let* ((slot (1- attnum))
               (column (if (eq (first (third expression)) :col)
                           (third expression) (fourth expression)))
               (type (expr-type column))
               (test (cdr (assoc operator '(("=" . =) ("<" . <) ("<=" . <=)
                                            (">" . >) (">=" . >=))
                                 :test #'string=)))
               (comparison
                 (when (indexable-equality-value-p literal type)
                   (case type
                     ((:float4 :float8) #'sql-float-compare)
                     (:uuid #'sql-uuid-compare)
                     (:timetz #'sql-timetz-compare)
                     (:numeric #'sql-numeric-compare)))))
          (flet ((strict (function)
                   (lambda (values)
                     (let ((value (aref values slot)))
                       (if (eq value +null+) +null+ (funcall function value))))))
            (cond
              ((and (integerp literal)
                    (member type '(:int2 :int4 :int8 :oid :date :time :timestamp :timestamptz)))
               (strict (lambda (value) (funcall test value literal))))
              ((and (member type '(:text :varchar)) (stringp literal) (string= operator "="))
               (strict (lambda (value) (string= value literal))))
              ((and (eq type :bool) (typep literal 'boolean) (string= operator "="))
               (strict (lambda (value) (eq value literal))))
              ((and (eq type :interval) (sql-interval-p literal))
               (let ((constant (sql-interval-comparison-value literal)))
                 (strict (lambda (value)
                           (funcall test (sql-interval-comparison-value value) constant)))))
              (comparison
               (strict (lambda (value) (funcall test (funcall comparison value literal) 0))))
              (t (lambda (values) (sql-compare operator (aref values slot) literal))))))))))

(defun prepared-temporal-row-comparison (expression)
  "Evaluate literal date or timestamp arithmetic once, at the first comparison.
Deferring evaluation preserves errors and their short-circuit behavior."
  (let* ((operator (second expression))
         (left (third expression))
         (right (fourth expression))
         (column-first-p (eq (first left) :col))
         (column (if column-first-p left right))
         (constant (if column-first-p right left)))
    (when (and (eq (first column) :col)
               (member operator '("=" "<" "<=" ">" ">=") :test #'string=)
               (eq (first constant) :binop)
               (member (second constant) '("+" "-") :test #'string=)
               (member (expr-type constant) '(:date :timestamp))
               (every (lambda (operand)
                        (and (eq (first operand) :const)
                             (member (expr-type operand)
                                     '(:date :timestamp :interval :int2 :int4 :int8))))
                      (list (third constant) (fourth constant))))
      (let ((comparison nil))
        (lambda (values)
          (unless comparison
            (let ((literal (list :const (eval-expr constant #()) :type (expr-type constant))))
              (setf comparison
                    (prepared-row-comparison
                     (if column-first-p
                         (list :binop operator column literal)
                         (list :binop operator literal column))))))
          (funcall comparison values))))))

(defun hashed-row-membership-type-p (type)
  (member type '(:int2 :int4 :int8 :date :time :timestamp :timestamptz
                 :bool :text :varchar)))

(defun prepared-row-membership (expression)
  "Build a lookup for constant IN lists where hash equality is SQL equality."
  (let* ((properties (nthcdr 4 expression))
         (left (getf properties :array-left))
         (items (getf properties :array-items))
         (type (expr-type left)))
    (when (and (eq (first left) :col) items (null (getf properties :comparisons))
               (hashed-row-membership-type-p type)
               (every (lambda (item)
                        (and (eq (first item) :const)
                             (or (eq (second item) +null+)
                                 (indexable-equality-value-p (second item) type))))
                      items))
      (let ((seen (make-hash-table :test (if (member type '(:text :varchar)) 'equal 'eql)))
            (slot (1- (second left)))
            (negated-p (fourth expression))
            (null-p nil))
        (dolist (item items)
          (if (eq (second item) +null+)
              (setf null-p t)
              (setf (gethash (second item) seen) t)))
        (lambda (values)
          (let* ((value (aref values slot))
                 (result (cond ((eq value +null+) +null+)
                               ((gethash value seen) t)
                               (null-p +null+))))
            (if negated-p (sql-3val-not result) result)))))))

(defun prepared-row-qualification (expression)
  "Prepare pure comparisons once while keeping SQL's three-valued evaluation."
  (case (first expression)
    (:binop (or (prepared-row-comparison expression)
                (prepared-temporal-row-comparison expression)))
    (:in (prepared-row-membership expression))
    ((:and :or)
     (let ((left (prepared-row-qualification (second expression)))
           (right (prepared-row-qualification (third expression))))
       (when (and left right)
         (if (eq (first expression) :and)
             (lambda (values)
               (let ((value (funcall left values)))
                 (when value (sql-3val-and value (funcall right values)))))
             (lambda (values)
               (let ((value (funcall left values)))
                 (if (eq value t) t
                     (sql-3val-or value (funcall right values)))))))))))

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
         (target (first (select-scope-relations (getf prepared :scope))))
         (target-presence (vector (select-relation-identity target)))
         (where (or (getf prepared :dml-row-where) (getf prepared :where)))
         ;; A sole built-in integer equality needs no source row, datum origins
         ;; or TOAST. Reject nonmatches before deforming unrelated attributes.
         (columns (pg-class-columns table))
         (equality (when (and (null store)
                              (some (lambda (column)
                                      (heap-varlena-type-p (pg-column-type column)))
                                    columns))
                     (multiple-value-list (equality-column-constant where))))
         (slot (destructuring-bind (&optional attnum literal matched-p) equality
                 (when (and matched-p (integerp literal)
                            (<= 1 attnum (length columns))
                            (member (pg-column-type (aref columns (1- attnum)))
                                    '(:int2 :int4 :int8)))
                   (1- attnum))))
         (qualification (when (and (null plan) (null (pg-class-toast table)))
                          (prepared-row-qualification where)))
         (raw-filter (when qualification (prepared-single-column-heap-filter table where)))
         (values-filter (when qualification
                          (lambda (values)
                            (check-for-interrupts)
                            (true-p (funcall qualification values)))))
         (prefilter (when (and slot (null qualification))
                      (let ((literal (second equality)))
                        (lambda (values)
                          (check-for-interrupts)
                          (eql (aref values slot) literal))))))
    (unwind-protect
         (do-table-tuples (tuple table :prefilter prefilter :demand slot
                                       :values-filter values-filter :raw-filter raw-filter)
           (materialize-select-tuple table tuple *current-snapshot*
                                     :demand toast-demand :retain-external-p (not (null toast-demand)))
           (let ((modified-p nil))
             (flet ((offer (source)
                      (let* ((environment (if store
                                              (concatenate 'vector (htup-values tuple) source)
                                              (htup-values tuple)))
                             (*query-row-presence*
                               (if store
                                   (combine-query-row-presence target-presence *query-row-presence*)
                                   target-presence))
                             (*query-row-origins*
                               (and *datum-origin-capture*
                                    (dml-row-origins tuple *query-row-origins* environment))))
                        (when (or qualification (null where)
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
