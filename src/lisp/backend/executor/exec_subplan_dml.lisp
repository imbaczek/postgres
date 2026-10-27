;;;; exec_subplan_dml.lisp -- statement-owned subqueries in mutation expressions.
(in-package #:pg)

(defun raw-query-sublinks-p (form)
  (and (consp form)
       (or (eq (first form) :sublink)
           (raw-query-sublinks-p (car form))
           (raw-query-sublinks-p (cdr form)))))

(defun analyze-subquery-assignment (expression column)
  (let ((source (expr-type expression))
        (target (pg-column-type column))
        (typmod (pg-column-typmod column)))
    (when (in-unknown-constant-p expression)
      (setf expression (coerce-in-constant expression target)
            source target))
    (unless (or (eq source target)
                (eq target :varchar)
                (and (member target '(:int4 :int8))
                     (member source '(:int2 :int4 :int8 :numeric :oid))))
      (ereport "42804" "column \"~a\" is of type ~a but expression is of type ~a"
               (pg-column-name column) target source))
    (list :assignment expression :type target :typmod typmod)))

(defun eval-subquery-assignment (expression environment)
  (let* ((argument (second expression))
         (target (expr-type expression))
         (typmod (expr-typmod expression))
         (value (sql-cast-value (eval-expr argument environment)
                                (expr-type argument) target -1)))
    (when (and (eq target :varchar) (not (eq value +null+)) (>= typmod 4))
      (let ((maximum (- typmod 4)))
        (when (> (length value) maximum)
          (unless (every (lambda (character) (char= character #\Space))
                         (subseq value maximum))
            (ereport "22001" "value too long for type character varying(~a)" maximum))
          (setf value (subseq value 0 maximum)))))
    value))

(defun prepare-dml-subqueries (ast)
  (let ((prepared
          (with-write-lock
            (let* ((table (resolve-table-or-error (getf ast :table)))
                   (columns (pg-class-columns table))
                   (*select-outer-scope* nil)
                   (*select-analysis-frame*
                     (list :parent nil :descriptors (make-array 0 :adjustable t :fill-pointer 0)
                           :group-keys nil :kind :dml :inside-input nil))
                   (*select-aggregate-analysis* *select-analysis-frame*)
                   (*select-sublinks-allowed-p* t)
                   (*select-sublinks* nil)
                   (*select-set-functions-allowed-p* nil)
                   (*inside-aggregate-input-p* nil)
                   (*aggregate-order-mode* nil)
                   (scope (if (eq (getf ast :type) :insert) (make-select-scope)
                              (make-base-select-scope (getf ast :table) nil table)))
                   (rows nil) (sets nil) (attnums nil) (targets nil)
                   (where (when (getf ast :where)
                            (coerce-boolean-expression (analyze-expr (getf ast :where) scope) "WHERE"))))
              (flet ((analyze (raw column)
                       (let ((expression
                               (if (eq (first raw) :default) :default
                                   (analyze-subquery-assignment (analyze-expr raw scope) column))))
                         (push (list :expr expression) targets)
                         expression)))
                (when (eq (getf ast :type) :insert)
                  (let ((names (getf ast :columns)))
                    (when (and names (/= (length names)
                                         (length (remove-duplicates names :test #'string=))))
                      (ereport "42701" "column specified more than once"))
                    (setf attnums (if names
                                      (mapcar (lambda (name)
                                                (pg-column-attnum (resolve-column-or-error table name)))
                                              names)
                                      (loop for index from 1 to (length columns) collect index))))
                  (setf rows
                        (mapcar (lambda (row)
                                  (unless (= (length row) (length attnums))
                                    (ereport "42601" "INSERT has more target columns than expressions"))
                                  (loop for raw in row for attnum in attnums
                                        collect (analyze raw (aref columns (1- attnum)))))
                                (getf ast :rows))))
                (setf sets
                      (mapcar (lambda (assignment)
                                (let ((column (resolve-column-or-error table (getf assignment :column))))
                                  (cons column (analyze (getf assignment :expr) column))))
                              (getf ast :sets))))
              (list :sublinks (nreverse *select-sublinks*) :table table :columns columns :scope scope :source-relations nil
                    :relations nil :where where :targets (nreverse targets)
                    :rows rows :sets sets :attnums attnums)))))
    (clear-query-analysis-contexts prepared)
    (plan-expression-subqueries prepared)
    prepared))

(defun revalidate-subquery-relations (relations)
  (dolist (relation relations)
    (unless (getf relation :virtual-p)
      (let* ((table (getf relation :table))
             (path (getf relation :path))
             (index (and (eq (select-scan-path-kind path) :index-scan)
                         (select-scan-path-index path))))
        (ensure-current-table table)
        (unless (eq (getf relation :columns) (pg-class-columns table))
          (ereport "0A000" "table definition changed during query planning"))
        (when (and index (not (select-index-current-p table index)))
          (setf (getf relation :path) (make-select-scan-path :kind :seq-scan))))))
  relations)

(defun call-with-dml-subqueries (prepared function)
  ;; Register target writes and subquery reads together before catalog revalidation.
  ;; Target writer registration also excludes concurrent index DDL.
  (let* ((table (getf prepared :table))
         (relfilenode (pg-class-relfilenode table))
         (relations (mapcar #'copy-list (expression-subquery-relations prepared)))
         (xid (get-current-xid))
         (snapshot *current-snapshot*))
    (register-relation-accesses (list relfilenode)
                                (select-relation-reader-relfilenodes relations) xid)
    (with-write-lock
      (ensure-current-table table)
      (unless (eq (getf prepared :columns) (pg-class-columns table))
        (ereport "0A000" "table definition changed during query planning"))
      (revalidate-subquery-relations relations))
    (let* ((*subquery-execution-active-p* t)
           (*subquery-result-cache* (make-hash-table :test #'eq))
           (*subquery-registered-relations* relations)
           (*subquery-outer-frames* nil)
           (*subquery-backend-entry* (query-plan-backend-entry :interpreter))
           (*current-snapshot* snapshot)
           (domain (query-plan-index-relfilenodes
                    (%make-query-plan :physical '(:result)) relations)))
      (call-with-select-index-lease-domain
       domain
       (lambda ()
         (detach-select-index-scans)
         (funcall function table relfilenode xid))))))

(defun call-with-dml-expression-evaluation (function)
  ;; Release enclosing leases before evaluation; child plans acquire their own
  ;; groups. Discharge remaining leases on exit before returning to mutation.
  (detach-select-index-scans)
  (unwind-protect (funcall function)
    (detach-select-index-scans)))

(defun stream-subquery-dml-candidates (table where function)
  ;; Evaluate each candidate as it arrives, preserving volatile expression cadence.
  (do-table-tuples (tuple table)
    (when (or (null where)
              (call-with-dml-expression-evaluation
               (lambda () (true-p (eval-expr where (htup-values tuple))))))
      (funcall function tuple))))

(defun subquery-update-values (table sets old)
  (let* ((columns (pg-class-columns table))
         (new (copy-seq old)))
    (dolist (assignment sets)
      (let* ((column (car assignment))
             (expression (cdr assignment))
             (value (if (eq expression :default) (insert-column-default column)
                        (eval-expr expression old))))
        (setf (aref new (1- (pg-column-attnum column)))
              (coerce-value value (pg-column-type column) (pg-column-typmod column)))))
    (check-not-null new columns)
    new))

(defun exec-insert-subqueries (stream ast)
  (let ((prepared (prepare-dml-subqueries ast)))
    (call-with-dml-subqueries
     prepared
     (lambda (table rel xid)
       (declare (ignore rel xid))
       (let ((columns (getf prepared :columns)) (count 0))
         (dolist (row (getf prepared :rows))
           (check-for-interrupts)
           (let ((values
                   (call-with-dml-expression-evaluation
                    (lambda ()
                      (let ((values (make-array (length columns) :initial-element +null+)))
                        (loop for expression in row for attnum in (getf prepared :attnums)
                              for column = (aref columns (1- attnum))
                              do (setf (aref values (1- attnum))
                                       (coerce-value
                                        (if (eq expression :default) (insert-column-default column)
                                            (eval-expr expression nil))
                                        (pg-column-type column) (pg-column-typmod column))))
                        (loop for column across columns for position from 1
                              unless (member position (getf prepared :attnums))
                                do (setf (aref values (1- position))
                                         (coerce-value (insert-column-default column)
                                                       (pg-column-type column)
                                                       (pg-column-typmod column))))
                        (check-not-null values columns)
                        values)))))
             (multiple-value-bind (block off publications)
                 (insert-tuple-with-unique table values :publication-kind :insert)
               (declare (ignore block off))
               (logical-note-row-change :insert table :new-values values
                                        :publication-selection publications))
             (incf count)))
         (send-command-complete stream (format nil "INSERT 0 ~a" count)))))))

(defun exec-delete-subqueries (stream ast)
  (let* ((prepared (prepare-dml-subqueries ast))
         (where (getf prepared :where)))
    (call-with-dml-subqueries
     prepared
     (lambda (table rel xid)
       (let ((columns (getf prepared :columns)) (count 0))
         (stream-subquery-dml-candidates
          table where
          (lambda (candidate)
            (check-for-interrupts)
            (detach-select-index-scans)
            (multiple-value-bind (deleted tuple block off status publications)
                (heapam-delete-tid
                 rel columns (htup-block candidate) (htup-offnum candidate) xid
                 (backend-command-id *backend*)
                 (lambda (tuple)
                   (call-with-dml-expression-evaluation
                    (lambda () (or (null where) (true-p (eval-expr where (htup-values tuple)))))))
                 (lambda () (logical-capture-publications (list table) :delete))
                 t candidate)
              (declare (ignore block off))
              (check-heapam-write-status status)
              (when deleted
                (logical-note-row-change :delete table :old-values (htup-values tuple)
                                         :publication-selection publications)
                (incf count)))))
         (send-command-complete stream (format nil "DELETE ~a" count)))))))

(defun exec-update-subqueries (stream ast)
  (let* ((prepared (prepare-dml-subqueries ast))
         (where (getf prepared :where))
         (sets (getf prepared :sets)))
    (call-with-dml-subqueries
     prepared
     (lambda (table rel xid)
       (let ((columns (getf prepared :columns)) (count 0))
         (stream-subquery-dml-candidates
          table where
          (lambda (candidate)
            (check-for-interrupts)
            (let ((new (call-with-dml-expression-evaluation
                        (lambda () (subquery-update-values table sets (htup-values candidate))))))
              (multiple-value-bind (deleted tuple old-block old-offnum status)
                  (heapam-delete-tid
                   rel columns (htup-block candidate) (htup-offnum candidate) xid
                   (backend-command-id *backend*)
                   (lambda (tuple)
                     (call-with-dml-expression-evaluation
                      (lambda ()
                        (when (or (null where) (true-p (eval-expr where (htup-values tuple))))
                          (setf new (subquery-update-values table sets (htup-values tuple)))
                          t))))
                   nil t candidate)
                (check-heapam-write-status status)
                (when deleted
                  (multiple-value-bind (block off publications)
                      (insert-tuple-with-unique
                       table new :exclude-tid (cons old-block old-offnum)
                       :publication-kind :update
                       :on-success
                       (lambda (block off)
                         (unless (heapam-set-ctid-unlocked rel old-block old-offnum block off)
                           (ereport "XX000" "could not link updated heap tuple"))))
                    (declare (ignore block off))
                    (logical-note-row-change :update table :old-values (htup-values tuple)
                                             :new-values new :publication-selection publications))
                  (incf count))))))
         (send-command-complete stream (format nil "UPDATE ~a" count)))))))
