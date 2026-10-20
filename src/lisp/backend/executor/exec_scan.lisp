;;;; exec_scan.lisp -- SELECT access-path selection and scan execution.

(in-package #:pg)

(defmacro do-table-tuples ((tuple table &key relfilenode) &body body)
  "Run BODY for visible tuples in TABLE, checking interrupts per tuple.

An already-bound RELFILENODE may be supplied when relation registration must
precede scanning. Registration and locking remain the caller's responsibility."
  (let ((table-value (gensym "TABLE-"))
        (rel (gensym "RELFILENODE-"))
        (columns (gensym "COLUMNS-"))
        (snapshot (gensym "SNAPSHOT-")))
    `(let* ((,table-value ,table)
            (,rel ,(or relfilenode `(pg-class-relfilenode ,table-value)))
            (,columns (pg-class-columns ,table-value))
            (,snapshot *current-snapshot*))
       (heapam-scan ,rel ,columns ,snapshot
                    (lambda (,tuple)
                      (check-for-interrupts)
                      ,@body)))))

(defmacro with-registered-table ((table relfilenode xid mode) &body body)
  "Register TABLE for the transaction, then revalidate it under the write lock.

MODE is the literal :READ or :WRITE. Registration remains until the XID ends."
  (let ((registration-function
          (ecase mode
            (:read 'register-relation-reader)
            (:write 'register-relation-writer)))
        (table-value (gensym "TABLE-"))
        (relfilenode-value (gensym "RELFILENODE-"))
        (xid-value (gensym "XID-")))
    `(let* ((,table-value ,table)
            (,relfilenode-value (pg-class-relfilenode ,table-value))
            (,xid-value (get-current-xid)))
       (,registration-function ,relfilenode-value ,xid-value)
       (with-write-lock
         (ensure-current-table ,table-value))
       (let ((,relfilenode ,relfilenode-value)
             (,xid ,xid-value))
         (declare (ignorable ,relfilenode ,xid))
         ,@body))))

(defun target->result-column (target)
  (let ((oid (type-oid (getf target :type))))
    (make-result-column :name (getf target :name)
                        :table-oid (getf target :table-oid)
                        :attnum (getf target :attnum)
                        :type-oid oid
                        :typlen (type-typlen oid)
                        :typmod (getf target :typmod)
                        :format 0)))

(defun order-compare (a b)
  (cond
    ((and (eq a +null+) (eq b +null+)) 0)
    ((eq a +null+) 1)
    ((eq b +null+) -1)
    (t (compare-3way a b))))

(defun row-less-p (a b order)
  (loop for (expr . desc) in order
        do (let* ((va (eval-expr expr (cdr a)))
                  (vb (eval-expr expr (cdr b)))
                  (c (order-compare va vb)))
             (unless (zerop c)
               (return (if desc (> c 0) (< c 0)))))
        finally (return nil)))

(defstruct select-scan-path
  (kind :seq-scan)
  index
  qual
  key)

(defun where-conjuncts (where)
  "Return WHERE's top-level AND terms in source order."
  (if (and where (eq (first where) :and))
      (append (where-conjuncts (second where))
              (where-conjuncts (third where)))
      (if where (list where) nil)))

(defun simple-equality-constant (expr)
  (case (first expr)
    (:const
     (values (second expr) t))
    (:unary
     (let ((value (third expr)))
       (when (and (string= (second expr) "-")
                  (eq (first value) :const)
                  (integerp (second value)))
         (values (- (second value)) t))))))

(defun equality-column-constant (qual)
  (when (and (eq (first qual) :binop)
             (string= (second qual) "="))
    (let ((left (third qual))
          (right (fourth qual)))
      (multiple-value-bind (value constant-p)
          (simple-equality-constant right)
        (when (and (eq (first left) :col) constant-p)
          (return-from equality-column-constant
            (values (second left) value t))))
      (multiple-value-bind (value constant-p)
          (simple-equality-constant left)
        (when (and (eq (first right) :col) constant-p)
          (values (second right) value t))))))

(defun indexable-equality-value-p (value type)
  (and (not (eq value +null+))
       (case type
         ((:int4 :int8) (integerp value))
         (:varchar (stringp value))
         (t nil))))

(defun index-path-for-equality (table qual)
  (multiple-value-bind (attnum value found-p)
      (equality-column-constant qual)
    (when found-p
      (dolist (idx (table-indexes table))
        (when (and (pg-index-valid-p idx)
                   (= (length (pg-index-columns idx)) 1)
                   (= (first (pg-index-columns idx)) attnum))
          (let* ((types (index-types idx table))
                 (type (first types)))
            (when (indexable-equality-value-p value type)
              (return-from index-path-for-equality
                (make-select-scan-path
                 :kind :index-scan
                 :index idx
                 :qual qual
                 :key (encode-index-key (list value) types))))))))
    nil))

(defun choose-select-scan-path (table where)
  "Choose the first single-column equality index usable by a top-level AND."
  (or (loop for qual in (where-conjuncts where)
            for path = (index-path-for-equality table qual)
            when path return path)
      (make-select-scan-path :kind :seq-scan)))

(defun select-relation-columns (relation)
  (loop for column across (pg-class-columns (select-relation-table relation))
        collect (let* ((attnum (pg-column-attnum column))
                       (expr (list :col (+ (select-relation-offset relation)
                                           attnum)
                                   :type (pg-column-type column)
                                   :typmod (pg-column-typmod column)
                                   :table-oid
                                   (pg-class-oid (select-relation-table relation))
                                   :attnum attnum)))
                  (make-select-column
                   :name (pg-column-name column)
                   :expr expr
                   :type (pg-column-type column)
                   :typmod (pg-column-typmod column)
                   :table-oid (pg-class-oid (select-relation-table relation))
                   :attnum attnum))))

(defun make-base-select-scope (name alias table)
  (let* ((width (length (pg-class-columns table)))
         (relation (make-select-relation :name name :alias alias
                                         :table table :offset 0 :width width)))
    (make-select-scope :relations (list relation)
                       :columns (select-relation-columns relation)
                       :width width)))

(defun extend-select-scope (scope relation)
  (let* ((qualifier (select-relation-qualifier relation))
         (duplicate
           (find qualifier (select-scope-relations scope)
                 :key #'select-relation-qualifier :test #'string=)))
    (when duplicate
      (ereport "42712" "table name \"~a\" specified more than once"
               qualifier))
    (make-select-scope
     :relations (append (select-scope-relations scope) (list relation))
     :columns (append (select-scope-columns scope)
                      (select-relation-columns relation))
     :width (+ (select-scope-width scope) (select-relation-width relation)))))

(defun visible-column-by-name (columns name)
  (let ((matches (remove-if-not
                  (lambda (column)
                    (string= name (select-column-name column)))
                  columns)))
    (cond
      ((null matches)
       (ereport "42703" "column \"~a\" specified in USING clause does not exist"
                name))
      ((rest matches)
       (ereport "42702" "column reference \"~a\" is ambiguous" name))
      (t (first matches)))))

(defun common-using-type (left-type right-type name)
  (cond
    ((eq left-type right-type) left-type)
    ((and (member left-type '(:int4 :int8))
          (member right-type '(:int4 :int8)))
     :int8)
    (t
     (ereport "42804" "JOIN/USING types ~a and ~a cannot be matched for column ~a"
              left-type right-type name))))

(defun common-using-typmod (left right common-type)
  (if (and (eq (select-column-type left) common-type)
           (eq (select-column-type right) common-type)
           (= (select-column-typmod left) (select-column-typmod right)))
      (select-column-typmod left)
      -1))

(defun using-column-expression (column type typmod)
  (let ((expr (select-column-expr column)))
    (if (and (eq (select-column-type column) type)
             (= (select-column-typmod column) typmod))
        expr
        (list :join-relabel expr :type type :typmod typmod))))

(defun using-column-pairs (left-columns right-columns names)
  (when (null names)
    (ereport "42601" "USING requires at least one column"))
  (let ((seen '()))
    (loop for name in names
          collect
          (progn
            (when (member name seen :test #'string=)
              (ereport "42701" "column \"~a\" specified more than once in USING clause"
                       name))
            (push name seen)
            (let* ((left (visible-column-by-name left-columns name))
                   (right (visible-column-by-name right-columns name))
                   (type (common-using-type (select-column-type left)
                                            (select-column-type right)
                                            name)))
              (list :left left :right right :type type
                    :typmod (common-using-typmod left right type)))))))

(defun using-join-qual (pairs)
  (let ((equalities
          (mapcar (lambda (pair)
                    (list :binop "="
                          (select-column-expr (getf pair :left))
                          (select-column-expr (getf pair :right))
                          :type :bool :typmod -1))
                  pairs)))
    (reduce (lambda (left right)
              (list :and left right :type :bool :typmod -1))
            equalities)))

(defun using-output-column (name pair join-type)
  (let* ((left (getf pair :left))
         (right (getf pair :right))
         (type (getf pair :type))
         (typmod (getf pair :typmod))
         ;; C's buildMergedJoinVar relabels only inputs whose type or typmod
         ;; differs from the common output. Keep plain Vars plain so origin
         ;; metadata remains available when the selected side needs no cast.
         (left-expr (using-column-expression left type typmod))
         (right-expr (using-column-expression right type typmod))
         (expr (case join-type
                 (:right right-expr)
                 (:full (list :join-coalesce left-expr right-expr
                              :type type :typmod typmod))
                 (:inner (cond ((eq (first left-expr) :col) left-expr)
                               ((eq (first right-expr) :col) right-expr)
                               (t left-expr)))
                 (t left-expr))))
    (make-select-column
     :name name :expr expr :type type :typmod typmod
     ;; PostgreSQL does not mark a merged JOIN USING alias as a direct
     ;; reference unless the selected expression remains a plain base Var.
     :table-oid (if (eq (first expr) :col) (or (getf expr :table-oid) 0) 0)
     :attnum (if (eq (first expr) :col)
                 (or (getf expr :attnum) (second expr))
                 0))))

(defun using-output-columns (left-columns right-columns names pairs join-type)
  (append
   (loop for name in names
         for pair in pairs
         collect (using-output-column name pair join-type))
   (remove-if (lambda (column)
                (member (select-column-name column) names :test #'string=))
              left-columns)
   (remove-if (lambda (column)
                (member (select-column-name column) names :test #'string=))
              right-columns)))

(defun select-table-or-error (name)
  (let ((table (lookup-table name)))
    (unless table
      (ereport "42P01" "relation \"~a\" does not exist" name))
    table))

(defun prepare-select (ast)
  ;; Resolve all names on one catalog view. Reader registration below protects
  ;; that view from DDL before any heap or index page is scanned.
  (with-write-lock
    (let* ((from-name (getf ast :from))
           (from-alias (getf ast :from-alias))
           (first-table (when from-name (select-table-or-error from-name)))
           (scope (when first-table
                    (make-base-select-scope from-name from-alias first-table)))
           (relations (when scope
                        (list (list :name from-name :alias from-alias
                                    :table first-table
                                    :columns (pg-class-columns first-table)
                                    :path (make-select-scan-path
                                           :kind :seq-scan)))))
           (joins '()))
      (dolist (join-ast (getf ast :joins))
        (let* ((name (getf join-ast :table))
               (alias (getf join-ast :alias))
               (table (select-table-or-error name))
               (offset (select-scope-width scope))
               (relation (make-select-relation
                          :name name :alias alias :table table :offset offset
                          :width (length (pg-class-columns table))))
               (right-columns (select-relation-columns relation))
               (join-type (getf join-ast :type))
               (using-names (and (getf join-ast :using)
                                 (getf join-ast :condition)))
               (pairs (when using-names
                        (using-column-pairs (select-scope-columns scope)
                                            right-columns using-names)))
               (join-scope (extend-select-scope scope relation))
               (qual (cond
                       ((eq join-type :cross) '(:bool t))
                       (using-names (using-join-qual pairs))
                       (t (analyze-expr (getf join-ast :condition)
                                        join-scope))))
               (columns (if using-names
                            (using-output-columns
                             (select-scope-columns scope) right-columns
                             using-names pairs join-type)
                            (select-scope-columns join-scope))))
          (setf scope (make-select-scope
                       :relations (select-scope-relations join-scope)
                       :columns columns
                       :width (select-scope-width join-scope)))
          (push (list :type join-type :qual qual :left-width offset
                      :right-width (select-relation-width relation))
                joins)
          (setf relations
                (append relations
                        (list (list :name name :alias alias :table table
                                    :columns (pg-class-columns table)
                                    :path (make-select-scan-path
                                           :kind :seq-scan)))))))
      (setf joins (nreverse joins))
      (let* ((where (when (getf ast :where)
                      (analyze-expr (getf ast :where) scope)))
             (single-table-p (and (= (length relations) 1)
                                  (null joins)))
             (path (if single-table-p
                       (choose-select-scan-path first-table where)
                       (if relations
                           (make-select-scan-path :kind :seq-scan)
                           (make-select-scan-path :kind :result))))
             (targets (analyze-targets (getf ast :targets) scope))
             (order (analyze-order (getf ast :order-by) scope targets))
             (limit (getf ast :limit)))
        (when single-table-p
          (setf (getf (first relations) :path) path))
        (list :table first-table
              :relations relations
              :scope scope
              :targets targets
              :where where
              :order order
              :limit limit
              :path path
              :joins joins)))))
(defun select-index-current-p (table idx)
  (and (= (pg-index-table-oid idx) (pg-class-oid table))
       (member idx (table-indexes table) :test #'eq)))

(defmacro with-registered-select-relations ((registered-relations relations)
                                            &body body)
  "Protect every relation and access path before evaluating BODY.

All heap/index reader registrations happen before the catalog mutex is taken
to revalidate the planned table and index identities.  This keeps the
registration-to-DDL handoff free of a lock-order inversion."
  (let ((relation-list (gensym "RELATIONS-"))
        (xid (gensym "XID-"))
        (spec (gensym "SPEC-"))
        (table (gensym "TABLE-"))
        (path (gensym "PATH-"))
        (index (gensym "INDEX-")))
    `(let* ((,relation-list (mapcar #'copy-list ,relations))
            (,xid (get-current-xid)))
       (dolist (,spec ,relation-list)
         (let* ((,table (getf ,spec :table))
                (,path (getf ,spec :path))
                (,index (and (eq (select-scan-path-kind ,path) :index-scan)
                             (select-scan-path-index ,path))))
           (register-relation-reader (pg-class-relfilenode ,table) ,xid)
           (when ,index
             (register-relation-reader (pg-index-relfilenode ,index) ,xid))))
       (with-write-lock
         (dolist (,spec ,relation-list)
           (let* ((,table (getf ,spec :table))
                  (,path (getf ,spec :path))
                  (,index (and (eq (select-scan-path-kind ,path) :index-scan)
                               (select-scan-path-index ,path))))
             (ensure-current-table ,table)
             (unless (eq (getf ,spec :columns) (pg-class-columns ,table))
               (ereport "0A000"
                        "table definition changed during query planning"))
             (when (and ,index (not (select-index-current-p ,table ,index)))
               ;; DDL completed after planning.  The old index reader remains
               ;; registered until transaction end; use SeqScan for this query.
               (setf (getf ,spec :path)
                     (make-select-scan-path :kind :seq-scan))))))
       (let ((,registered-relations ,relation-list))
         ,@body))))

(defun scan-select-index (table path snapshot callback)
  (let* ((idx (select-scan-path-index path))
         (relfilenode (pg-class-relfilenode table))
         ;; B-tree page cleanup and splits use the write lock.  Copy all
         ;; candidate TIDs while holding it, then fetch heap tuples unlocked.
         (tids (with-write-lock
                 (btree-search (pg-index-relfilenode idx)
                               (select-scan-path-key path)))))
    (dolist (tid (remove-duplicates tids :test #'equal))
      (check-for-interrupts)
      (let ((tuple (heapam-get-tuple relfilenode (car tid) (cdr tid)
                                     (pg-class-columns table))))
        (when (and tuple (htup-visible-p tuple snapshot))
          (funcall callback tuple))))))

(defun scan-select-path (table relfilenode path snapshot callback)
  (ecase (select-scan-path-kind path)
    (:seq-scan
     (heapam-scan relfilenode (pg-class-columns table) snapshot callback))
    (:index-scan
     (scan-select-index table path snapshot callback))))

(defun collect-select-table-rows (table relfilenode path where targets)
  (let ((rows '()))
    (labels ((consider (tuple)
               (check-for-interrupts)
               (let ((env (htup-values tuple)))
                 ;; Index quals only produce candidates.  Recheck the complete
                 ;; WHERE expression before projection just like SeqScan.
                 (when (or (null where) (true-p (eval-expr where env)))
                   (push (cons (map 'vector
                                    (lambda (tg)
                                      (eval-expr (getf tg :expr) env))
                                    targets)
                               env)
                         rows)))))
      (scan-select-path table relfilenode path *current-snapshot* #'consider))
    (nreverse rows)))

(defun collect-select-relation-rows (relation)
  (let* ((table (getf relation :table))
         (path (getf relation :path))
         (rows '()))
    ;; Joined WHERE clauses stay above the join tree.  In particular, pushing
    ;; an outer-side filter into this scan could turn a matched row into a
    ;; fabricated NULL-extended row, so join inputs always use SeqScan for v1.
    (unless (eq (select-scan-path-kind path) :seq-scan)
      (ereport "XX000" "joined relation scan must use SeqScan"))
    (scan-select-path
     table (pg-class-relfilenode table) path *current-snapshot*
     (lambda (tuple)
       (check-for-interrupts)
       (push (htup-values tuple) rows)))
    (nreverse rows)))

(defun join-select-environments (left-rows right-rows join)
  "Execute one nested-loop join, tracking ON matches independently of WHERE."
  (let* ((join-type (getf join :type))
         (qual (getf join :qual))
         (left-width (getf join :left-width))
         (right-width (getf join :right-width))
         (null-left (make-array left-width :initial-element +null+))
         (null-right (make-array right-width :initial-element +null+))
         (matched-right (make-array (length right-rows) :initial-element nil))
         (rows '()))
    (dolist (left left-rows)
      (check-for-interrupts)
      (let ((matched-left nil))
        (loop for right in right-rows
              for right-index from 0
              for joined = (concatenate 'vector left right)
              do (check-for-interrupts)
                 (when (true-p (eval-expr qual joined))
                   ;; Match state comes from ON alone.  WHERE is applied after
                   ;; all joins, matching PostgreSQL's joinqual/otherqual split.
                   (setf matched-left t
                         (aref matched-right right-index) t)
                   (push joined rows)))
        (when (and (not matched-left)
                   (member join-type '(:left :full)))
          (push (concatenate 'vector left null-right) rows))))
    (when (member join-type '(:right :full))
      (loop for right in right-rows
            for right-index from 0
            unless (aref matched-right right-index)
              do (push (concatenate 'vector null-left right) rows)))
    (nreverse rows)))

(defun collect-select-joined-rows (relations joins where targets)
  (let ((rows (collect-select-relation-rows (first relations))))
    (loop for relation in (rest relations)
          for join in joins
          do (setf rows
                   (join-select-environments
                    rows (collect-select-relation-rows relation) join)))
    (loop for env in rows
          when (or (null where) (true-p (eval-expr where env)))
            collect
            (cons (map 'vector
                       (lambda (target)
                         (eval-expr (getf target :expr) env))
                       targets)
                  env))))

(defun exec-select (stream ast)
  (let* ((prepared (prepare-select ast))
         (relations (getf prepared :relations))
         (joins (getf prepared :joins))
         (targets (getf prepared :targets))
         (where (getf prepared :where))
         (order (getf prepared :order))
         (limit (getf prepared :limit))
         (rows
           (cond
             ((and relations joins)
              (with-registered-select-relations (registered relations)
                (collect-select-joined-rows registered joins where targets)))
             (relations
              (with-registered-select-relations (registered relations)
                (let* ((relation (first registered))
                       (table (getf relation :table)))
                  (collect-select-table-rows
                   table (pg-class-relfilenode table) (getf relation :path)
                   where targets))))
             (t
              (let ((values (map 'vector
                                 (lambda (tg)
                                   (eval-expr (getf tg :expr) nil))
                                 targets)))
                (if (or (null where) (true-p (eval-expr where nil)))
                    (list (cons values nil))
                    nil))))))
    (when order
      (setf rows (stable-sort rows
                              (lambda (a b) (row-less-p a b order)))))
    (when limit
      (setf rows (subseq rows 0 (min limit (length rows)))))
    (send-row-description stream (mapcar #'target->result-column targets))
    (dolist (row rows)
      (send-data-row-text
       stream
       (loop for v across (car row)
             for tg in targets
             collect (value->text v (getf tg :type)))))
    (send-command-complete stream (format nil "SELECT ~a" (length rows)))))

(defun select-path-description (table path)
  (case (select-scan-path-kind path)
    (:index-scan
     (format nil "Index Scan using ~a on ~a"
             (pg-index-name (select-scan-path-index path))
             (pg-class-name table)))
    (:seq-scan
     (format nil "Seq Scan on ~a" (pg-class-name table)))
    (:result "Result")))

(defun select-join-plan-description (joins)
  (let ((names (mapcar (lambda (join)
                         (case (getf join :type)
                           (:inner "Inner Join")
                           (:left "Left Join")
                           (:right "Right Join")
                           (:full "Full Join")
                           (:cross "Cross Join")))
                       joins)))
    (if (null (rest names))
        (format nil "Nested Loop ~a" (first names))
        (format nil "Nested Loop (~{~a~^, ~})" names))))

(defun exec-explain (stream ast)
  (let* ((prepared (prepare-select ast))
         (relations (getf prepared :relations))
         (joins (getf prepared :joins))
         (path (getf prepared :path))
         (description
           (cond
             (joins (select-join-plan-description joins))
             (relations
              (with-registered-select-relations (registered relations)
                (select-path-description
                 (getf (first registered) :table)
                 (getf (first registered) :path))))
             (t "Result"))))
    (when (and joins relations)
      (with-registered-select-relations (registered relations)
        (declare (ignore registered))))
    (when (and (null joins) (null relations))
      (setf description (select-path-description nil path)))
    (send-row-description
     stream
     (list (make-result-column :name "QUERY PLAN"
                               :type-oid +textoid+
                               :typlen -1
                               :typmod -1)))
    (send-data-row-text stream (list description))
    (send-command-complete stream "EXPLAIN")))
