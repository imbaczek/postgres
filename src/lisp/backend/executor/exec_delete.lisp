;;;; exec_delete.lisp -- DELETE and UPDATE (MVCC: old tuple gets xmax,
;;;; new version is inserted with the current xid).

(in-package #:pg)

(defun collect-matching-tids (table where)
  (let ((tids '()))
    (do-table-tuples (tuple table)
      (when (or (null where)
                (true-p (eval-expr where (htup-values tuple))))
        (push (cons (htup-block tuple) (htup-offnum tuple)) tids)))
    (nreverse tids)))

(defun check-heapam-write-status (status)
  ;; nodeModifyTable.c reports TM_Updated and TM_Deleted as serialization
  ;; failures when IsolationUsesXactSnapshot() is true.
  (case status
    (:serialization-update-conflict
     (ereport "40001" "could not serialize access due to concurrent update"))
    (:serialization-delete-conflict
     (ereport "40001" "could not serialize access due to concurrent delete")))
  (values))

(defun exec-delete (stream ast)
  (let* ((table (resolve-table-or-error (getf ast :table)))
         (where (when (getf ast :where)
                  (analyze-expr (getf ast :where) table))))
    ;; The transaction retains this write registration through completion.
    (with-registered-table (table rel xid :write)
      (let ((columns (pg-class-columns table))
            (count 0))
        (dolist (tid (collect-matching-tids table where))
          (multiple-value-bind (deleted tuple block offnum status)
              (heapam-delete-tid
               rel columns (car tid) (cdr tid) xid
               (backend-command-id *backend*)
               (lambda (tuple)
                 (or (null where)
                     (true-p (eval-expr where (htup-values tuple))))))
            (declare (ignore block offnum))
            (check-heapam-write-status status)
            (when deleted
              (logical-note-row-change :delete table
                                       :old-values (htup-values tuple))
              (incf count))))
        (send-command-complete stream (format nil "DELETE ~a" count))))))

(defun exec-update (stream ast)
  (let* ((table (resolve-table-or-error (getf ast :table)))
         (sets (loop for s in (getf ast :sets)
                     for col = (resolve-column-or-error
                                table (getf s :column))
                     collect (cons col (analyze-expr (getf s :expr) table))))
         (where (when (getf ast :where)
                  (analyze-expr (getf ast :where) table))))
    (with-registered-table (table rel xid :write)
      (let ((columns (pg-class-columns table))
            (count 0))
        (dolist (tid (collect-matching-tids table where))
          (check-for-interrupts)
          (multiple-value-bind (deleted tuple old-block old-offnum status)
              (heapam-delete-tid
               rel columns (car tid) (cdr tid) xid
               (backend-command-id *backend*)
               (lambda (candidate)
                 (or (null where)
                     (true-p (eval-expr where (htup-values candidate))))))
            (check-heapam-write-status status)
            (when deleted
              (let* ((old (htup-values tuple))
                     (new (copy-seq old)))
                (dolist (set sets)
                  (let* ((col (car set))
                         (expr (cdr set))
                         (value (eval-expr expr old)))
                    (setf (aref new (1- (pg-column-attnum col)))
                          (coerce-value value
                                        (pg-column-type col)
                                        (pg-column-typmod col)))))
                (check-not-null new columns)
                (multiple-value-bind (new-block new-offnum)
                    (insert-tuple-with-unique
                     table new :exclude-tid (cons old-block old-offnum))
                  (unless (heapam-set-ctid rel old-block old-offnum
                                           new-block new-offnum)
                    (ereport "XX000" "could not link updated heap tuple"))
                  (logical-note-row-change :update table
                                           :old-values old
                                           :new-values new)
                  (incf count))))))
        (send-command-complete stream (format nil "UPDATE ~a" count))))))
