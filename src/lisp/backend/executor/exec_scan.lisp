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

(defun prepare-select (ast)
  ;; Catalog metadata and index definitions are mutable during DDL. Keep table
  ;; lookup, expression analysis, and path selection on one consistent view.
  (with-write-lock
    (let* ((table-name (getf ast :from))
           (table (when table-name (lookup-table table-name))))
      (when (and table-name (null table))
        (ereport "42P01" "relation \"~a\" does not exist" table-name))
      (let* ((targets (analyze-targets (getf ast :targets) table))
             (where (when (getf ast :where)
                      (analyze-expr (getf ast :where) table)))
             (order (analyze-order (getf ast :order-by) table targets))
             (limit (getf ast :limit))
             (path (if table
                       (choose-select-scan-path table where)
                       (make-select-scan-path :kind :result))))
        (list :table-name table-name
              :table table
              :targets targets
              :where where
              :order order
              :limit limit
              :path path)))))

(defun select-index-current-p (table idx)
  (and (= (pg-index-table-oid idx) (pg-class-oid table))
       (member idx (table-indexes table) :test #'eq)))

(defun register-select-access-path (table path)
  "Register heap and index readers before taking the write lock to revalidate."
  (let* ((relfilenode (pg-class-relfilenode table))
         (xid (get-current-xid))
         (idx (and (eq (select-scan-path-kind path) :index-scan)
                   (select-scan-path-index path))))
    (register-relation-reader relfilenode xid)
    (when idx
      (register-relation-reader (pg-index-relfilenode idx) xid))
    (with-write-lock
      (ensure-current-table table)
      (when (and idx (not (select-index-current-p table idx)))
        ;; DDL completed after planning.  The old index reader registration is
        ;; harmless for this transaction; use the heap path for this query.
        (setf path (make-select-scan-path :kind :seq-scan))))
    (values relfilenode xid path)))

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

(defun exec-select (stream ast)
  (let* ((prepared (prepare-select ast))
         (table (getf prepared :table))
         (targets (getf prepared :targets))
         (where (getf prepared :where))
         (order (getf prepared :order))
         (limit (getf prepared :limit))
         (path (getf prepared :path))
         (rows
           (if table
               (multiple-value-bind (relfilenode xid current-path)
                   (register-select-access-path table path)
                 (declare (ignore xid))
                 (collect-select-table-rows table relfilenode current-path
                                            where targets))
               (let ((values (map 'vector
                                  (lambda (tg)
                                    (eval-expr (getf tg :expr) nil))
                                  targets)))
                 (if (or (null where) (true-p (eval-expr where nil)))
                     (list (cons values nil))
                     nil)))))
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

(defun exec-explain (stream ast)
  (let* ((prepared (prepare-select ast))
         (table (getf prepared :table))
         (path (getf prepared :path)))
    ;; Hold the same relation readers as SELECT while reporting the path.  DDL
    ;; either waits for this reader or completes first and makes us replan to
    ;; SeqScan during registration.
    (multiple-value-bind (relfilenode xid current-path)
        (if table
            (register-select-access-path table path)
            (values nil nil path))
      (declare (ignore relfilenode xid))
      (let ((description (select-path-description table current-path)))
        (send-row-description
         stream
         (list (make-result-column :name "QUERY PLAN"
                                   :type-oid +textoid+
                                   :typlen -1
                                   :typmod -1)))
        (send-data-row-text stream (list description))
        (send-command-complete stream "EXPLAIN")))))
