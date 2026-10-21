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

(defmacro with-registered-relation-access
    ((relfilenodes xid mode) relation-expression validation-form &body body)
  "Register relation users before validating catalog state under *WRITE-LOCK*.

MODE is the literal :READ or :WRITE. Readers register the complete set
atomically; writers must register exactly one relation."
  (let* ((captured-relfilenodes (gensym "CAPTURED-RELFILENODES-"))
         (register-form
           (ecase mode
             (:read
              `(register-relation-readers ,captured-relfilenodes ,xid))
             (:write
              `(progn
                 (unless (= (length ,captured-relfilenodes) 1)
                   (error "write registration requires exactly one relation"))
                 (register-relation-writer
                  (first ,captured-relfilenodes) ,xid))))))
    `(let* ((,captured-relfilenodes
              (remove-duplicates (copy-list ,relation-expression) :test #'=))
            (,relfilenodes ,captured-relfilenodes)
            (,xid (get-current-xid)))
       (declare (ignorable ,relfilenodes ,xid))
       ,register-form
       (with-write-lock ,validation-form)
       ,@body)))

(defmacro with-registered-table ((table relfilenode xid mode) &body body)
  "Register TABLE for the transaction, then revalidate it under the write lock.

MODE is the literal :READ or :WRITE. Registration remains until the XID ends."
  (let ((table-value (gensym "TABLE-"))
        (relfilenode-value (gensym "RELFILENODE-"))
        (registered-relfilenodes (gensym "REGISTERED-RELFILENODES-")))
    `(let* ((,table-value ,table)
            (,relfilenode-value (pg-class-relfilenode ,table-value)))
       (with-registered-relation-access
           (,registered-relfilenodes ,xid ,mode)
           (list ,relfilenode-value)
           (ensure-current-table ,table-value)
         (let ((,relfilenode ,relfilenode-value))
           ,@body)))))

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

(defstruct select-top-n-item
  row
  keys
  sequence)

(defstruct (select-top-n-collector (:constructor %make-select-top-n-collector))
  limit
  order
  heap
  next-sequence)

(defun make-select-top-n-collector (limit order)
  (%make-select-top-n-collector
   :limit limit
   :order order
   :heap (make-array (min limit 32) :adjustable t :fill-pointer 0)
   :next-sequence 0))

(defun select-top-n-push (heap item limit)
  (when (= (fill-pointer heap) (array-dimension heap 0))
    (setf heap
          (adjust-array heap
                        (min limit (max 1 (* 2 (array-dimension heap 0))))
                        :fill-pointer (fill-pointer heap))))
  (vector-push item heap)
  heap)

(defun select-top-n-item-less-p (a b order)
  (loop for entry in order
        for position from 0
        for c = (order-compare (aref (select-top-n-item-keys a) position)
                               (aref (select-top-n-item-keys b) position))
        unless (zerop c)
          return (if (cdr entry) (> c 0) (< c 0))
        finally
           (return (< (select-top-n-item-sequence a)
                      (select-top-n-item-sequence b)))))

(defun select-top-n-sift-up (heap order index)
  (when (plusp index)
    (let ((parent (floor (1- index) 2)))
      (when (select-top-n-item-less-p (aref heap parent) (aref heap index)
                                      order)
        (rotatef (aref heap index) (aref heap parent))
        (select-top-n-sift-up heap order parent)))))

(defun select-top-n-sift-down (heap order index)
  (let* ((left (+ (* 2 index) 1))
         (right (1+ left))
         (heap-length (length heap)))
    (when (< left heap-length)
      (let* ((worse-child
               (if (and (< right heap-length)
                        (select-top-n-item-less-p (aref heap left)
                                                  (aref heap right) order))
                   right
                   left)))
        (when (select-top-n-item-less-p (aref heap index)
                                        (aref heap worse-child) order)
          (rotatef (aref heap index) (aref heap worse-child))
          (select-top-n-sift-down heap order worse-child))))))

(defun offer-select-top-n-row (collector row)
  (let* ((order (select-top-n-collector-order collector))
         (heap (select-top-n-collector-heap collector))
         (limit (select-top-n-collector-limit collector))
         (sequence (select-top-n-collector-next-sequence collector))
         (item (make-select-top-n-item
                :row row
                :keys (map 'vector
                           (lambda (entry)
                             (eval-expr (car entry) (cdr row)))
                           order)
                :sequence sequence)))
    (incf (select-top-n-collector-next-sequence collector))
    (cond
      ((< (length heap) limit)
       (setf (select-top-n-collector-heap collector)
             (select-top-n-push heap item limit))
       (setf heap (select-top-n-collector-heap collector))
       (select-top-n-sift-up heap order (1- (length heap))))
      ((and (plusp limit)
            (select-top-n-item-less-p item (aref heap 0) order))
       (setf (aref heap 0) item)
       (select-top-n-sift-down heap order 0)))))

(defun select-top-n-collector-rows (collector)
  (mapcar #'select-top-n-item-row
          (stable-sort (coerce (select-top-n-collector-heap collector) 'list)
                       (lambda (a b)
                         (select-top-n-item-less-p
                          a b (select-top-n-collector-order collector))))))

(defun collect-select-top-n (streamer order limit)
  "Consume STREAMER while retaining at most LIMIT rows in ORDER BY order."
  (let ((collector (make-select-top-n-collector limit order)))
    (unless (zerop limit)
      (funcall streamer (lambda (row) (offer-select-top-n-row collector row))))
    (select-top-n-collector-rows collector)))

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
                                    :columns (pg-class-columns first-table)))))
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
                                    :columns (pg-class-columns table)))))))
      (setf joins (nreverse joins))
      (let* ((where (when (getf ast :where)
                      (analyze-expr (getf ast :where) scope)))
             (aggregate-p
               (or (getf ast :group-by)
                   (some (lambda (target)
                           (eq (first (getf target :expr)) :count-star))
                         (getf ast :targets))))
             (group-keys (when (getf ast :group-by)
                           (analyze-group-keys (getf ast :group-by) scope)))
             (targets (if aggregate-p
                          (analyze-aggregate-targets
                           (getf ast :targets) scope group-keys)
                          (analyze-targets (getf ast :targets) scope)))
             (aggregate-count
               (if aggregate-p
                   (count-if (lambda (target)
                               (getf target :aggregate-index))
                             targets)
                   0))
             (order (if aggregate-p
                        (analyze-aggregate-order
                         (getf ast :order-by) scope targets group-keys)
                        (analyze-order (getf ast :order-by) scope targets)))
             (limit (getf ast :limit)))
        (list :table first-table
              :relations relations
              :scope scope
              :targets targets
              :where where
              :group-keys group-keys
              :aggregate-p aggregate-p
              :aggregate-count aggregate-count
              :order order
              :limit limit
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
        (index (gensym "INDEX-"))
        (registered-relfilenodes (gensym "REGISTERED-RELFILENODES-")))
    `(let ((,relation-list (mapcar #'copy-list ,relations)))
       (with-registered-relation-access
           (,registered-relfilenodes ,xid :read)
           (loop for ,spec in ,relation-list
                 for ,table = (getf ,spec :table)
                 for ,path = (getf ,spec :path)
                 for ,index = (and (eq (select-scan-path-kind ,path) :index-scan)
                                   (select-scan-path-index ,path))
                 append (cons (pg-class-relfilenode ,table)
                              (when ,index
                                (list (pg-index-relfilenode ,index)))))
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
                       (make-select-scan-path :kind :seq-scan)))))
         (let ((,registered-relations ,relation-list))
           ,@body)))))

(defun scan-select-index (table path snapshot callback)
  (let* ((idx (select-scan-path-index path))
         (relfilenode (pg-class-relfilenode table))
         ;; B-tree synchronization is scoped to this index; candidate TIDs
         ;; remain subject to heap visibility and the full WHERE recheck.
         (tids (btree-search (pg-index-relfilenode idx)
                             (select-scan-path-key path))))
    (dolist (tid (remove-duplicates tids :test #'equal))
      (check-for-interrupts)
      (let ((tuple (heapam-get-tuple relfilenode (car tid) (cdr tid)
                                     (pg-class-columns table))))
        (when (and tuple (htup-visible-p tuple snapshot)
                   (eq (funcall callback tuple) :stop))
          (return-from scan-select-index :stop))))
    (values)))

(defun scan-select-path (table relfilenode path snapshot callback)
  (ecase (select-scan-path-kind path)
    (:seq-scan
     (heapam-scan relfilenode (pg-class-columns table) snapshot callback))
    (:index-scan
     (scan-select-index table path snapshot callback))))

(defun make-select-result-row (environment where targets)
  "Apply WHERE before projecting ENVIRONMENT, retaining it for ORDER BY."
  (when (or (null where) (true-p (eval-expr where environment)))
    (cons (map 'vector
               (lambda (target)
                 (eval-expr (getf target :expr) environment))
               targets)
          environment)))

(defun collect-select-no-from-rows (where targets)
  (let ((row (make-select-result-row nil where targets)))
    (if row (list row) nil)))

(defun collect-select-table-rows (table relfilenode path where targets)
  (let ((rows '()))
    (labels ((consider (tuple)
               (check-for-interrupts)
               (let ((env (htup-values tuple)))
                 ;; Index quals only produce candidates.  Recheck the complete
                 ;; WHERE expression before projection just like SeqScan.
                 (let ((row (make-select-result-row env where targets)))
                   (when row (push row rows))))))
      (scan-select-path table relfilenode path *current-snapshot* #'consider))
    (nreverse rows)))

(defun stream-select-join-environments (left-stream right-stream join callback)
  "Run a lazy nested-loop join with a spillable, replayable inner stream.

The inner stream is opened only after the first outer row arrives. Its rows are
cached as they are consumed, so the first joined row can be returned before the
inner input is exhausted and later outer rows can replay the same input."
  (let* ((join-type (getf join :type))
         (qual (getf join :qual))
         (left-width (getf join :left-width))
         (right-width (getf join :right-width))
         (null-left (make-array left-width :initial-element +null+))
         (null-right (make-array right-width :initial-element +null+))
         (right/full-p (member join-type '(:right :full)))
         (work-memory (max 1 *query-work-memory-bytes*))
         (spool-memory (if right/full-p
                           (max 1 (floor work-memory 2))
                           work-memory))
         (match-memory (max 1 (- work-memory spool-memory))))
    (with-query-temp-files ()
                           (with-query-row-spool (right-spool spool-memory)
                             (labels ((run-join (matched-right)
                                        (let ((right-loaded nil)
                                              (outer-seen nil)
                                              (stopped nil))
                                          (labels ((emit (environment)
                                                     (when (eq (funcall callback environment) :stop)
                                                       (setf stopped t)
                                                       :stop))
                                                   (process-right (left right right-index matched-left)
                                                     (check-for-interrupts)
                                                     (let ((joined (concatenate 'vector left right)))
                                                       (when (true-p (eval-expr qual joined))
                                                         ;; Outer-join match state comes from ON
                                                         ;; alone; WHERE remains above this node.
                                                         (setf (car matched-left) t)
                                                         (when matched-right
                                                           (query-match-map-mark
                                                            matched-right right-index))
                                                         (emit joined))))
                                                   (load-right-for-left (left matched-left)
                                                     (setf right-loaded t)
                                                     (funcall
                                                      right-stream
                                                      (lambda (right)
                                                        (let ((right-index
                                                                (query-row-spool-append
                                                                 right-spool right)))
                                                          (when matched-right
                                                            (let ((map-index
                                                                    (query-match-map-append
                                                                     matched-right)))
                                                              (unless (= right-index map-index)
                                                                (error "Join spool and match map lost row alignment"))))
                                                          (process-right left right right-index
                                                                         matched-left))))
                                                     (unless stopped
                                                       (query-row-spool-seal right-spool)))
                                                   (visit-left (left)
                                                     (unless stopped
                                                       (check-for-interrupts)
                                                       (setf outer-seen t)
                                                       (let ((matched-left (list nil)))
                                                         (if right-loaded
                                                             (map-query-row-spool
                                                              right-spool
                                                              (lambda (right right-index)
                                                                (process-right
                                                                 left right right-index
                                                                 matched-left)))
                                                             (load-right-for-left left matched-left))
                                                         (when (and (not stopped)
                                                                    (not (car matched-left))
                                                                    (member join-type '(:left :full)))
                                                           (emit (concatenate 'vector
                                                                              left null-right))))
                                                       (when stopped :stop)))
                                                   (emit-unmatched-rights ()
                                                     (if outer-seen
                                                         (progn
                                                           (query-match-map-finish matched-right)
                                                           (map-query-row-spool
                                                            right-spool
                                                            (lambda (right right-index)
                                                              (unless
                                                                  (query-match-map-matched-p
                                                                   matched-right right-index)
                                                                (emit (concatenate 'vector
                                                                                   null-left right))))))
                                                         ;; RIGHT/FULL JOIN with an empty outer
                                                         ;; input can stream the inner rows directly.
                                                         (funcall
                                                          right-stream
                                                          (lambda (right)
                                                            (emit (concatenate 'vector
                                                                               null-left right)))))))
                                            (funcall left-stream #'visit-left)
                                            (unless stopped
                                              (when right/full-p
                                                (emit-unmatched-rights)))
                                            (when stopped :stop)))))
                               (if right/full-p
                                   (with-query-match-map (matched-right match-memory)
                                     (run-join matched-right))
                                   (run-join nil)))))))

(defun stream-select-joined-environments (relations joins callback)
  (labels ((stream-level (level level-callback)
             (if (= level 1)
                 (let* ((relation (first relations))
                        (table (getf relation :table)))
                   (scan-select-path
                    table (pg-class-relfilenode table) (getf relation :path)
                    *current-snapshot*
                    (lambda (tuple)
                      (check-for-interrupts)
                      (funcall level-callback (htup-values tuple)))))
                 (let* ((relation (nth (1- level) relations))
                        (join (nth (- level 2) joins))
                        (table (getf relation :table))
                        (path (getf relation :path)))
                   ;; Joined WHERE clauses stay above the join tree. In
                   ;; particular, filtering the right input here could turn a
                   ;; matched row into a fabricated NULL-extended row.
                   (unless (eq (select-scan-path-kind path) :seq-scan)
                     (ereport "XX000" "joined relation scan must use SeqScan"))
                   (stream-select-join-environments
                    (lambda (left-callback)
                      (stream-level (1- level) left-callback))
                    (lambda (right-callback)
                      (scan-select-path
                       table (pg-class-relfilenode table) path
                       *current-snapshot*
                       (lambda (tuple)
                         (check-for-interrupts)
                         (funcall right-callback (htup-values tuple)))))
                    join level-callback)))))
    (when relations
      (stream-level (length relations) callback))))

(defun stream-select-joined-rows (relations joins where targets callback)
  (stream-select-joined-environments
   relations joins
   (lambda (environment)
     (check-for-interrupts)
     (let ((row (make-select-result-row environment where targets)))
       (when row (funcall callback row))))))

(defun collect-select-joined-rows (relations joins where targets)
  (let ((rows '()))
    (stream-select-joined-rows
     relations joins where targets
     (lambda (row)
       (push row rows)))
    (nreverse rows)))

(defun send-select-row (stream row targets)
  (send-data-row-text
   stream
   (loop for value across (car row)
         for target in targets
         collect (value->text value (getf target :type)))))

(defun select-path-description (table path)
  (case (select-scan-path-kind path)
    (:index-scan
     (format nil "Index Scan using ~a on ~a"
             (pg-index-name (select-scan-path-index path))
             (pg-class-name table)))
    (:seq-scan
     (format nil "Seq Scan on ~a" (pg-class-name table)))
    (:result "Result")))
