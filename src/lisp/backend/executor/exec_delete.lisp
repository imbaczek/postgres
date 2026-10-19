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

(defun exec-delete (stream ast)
  (let ((table (resolve-table-or-error (getf ast :table))))
    (let* ((where (when (getf ast :where)
                    (analyze-expr (getf ast :where) table)))
           (rel (pg-class-relfilenode table))
           (columns (pg-class-columns table))
           (xid (get-current-xid))
           (count 0))
      ;; Register before scanning so DROP TABLE cannot remove the relation
      ;; between resolution, the heap scan, and the eventual mutation.
      (register-relation-writer rel xid)
      (with-write-lock
        (ensure-current-table table))
      (dolist (tid (collect-matching-tids table where))
        (when (heapam-delete-tid rel columns (car tid) (cdr tid)
                                 xid (backend-command-id *backend*))
          (incf count)))
      (send-command-complete stream (format nil "DELETE ~a" count)))))

(defun exec-update (stream ast)
  (let ((table (resolve-table-or-error (getf ast :table))))
    (let* ((columns (pg-class-columns table))
           (rel (pg-class-relfilenode table))
           (sets (loop for s in (getf ast :sets)
                       for col = (resolve-column-or-error
                                  table (getf s :column))
                       collect (cons col (analyze-expr (getf s :expr) table))))
           (where (when (getf ast :where)
                    (analyze-expr (getf ast :where) table)))
           (xid (get-current-xid))
           (count 0))
      (register-relation-writer rel xid)
      (with-write-lock
        (ensure-current-table table))
      (dolist (tid (collect-matching-tids table where))
        (check-for-interrupts)
        (let ((tuple (heapam-get-tuple rel (car tid) (cdr tid) columns)))
          (when (and tuple
                     (htup-visible-p tuple *current-snapshot*))
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
              (when (heapam-delete-tid rel columns (car tid) (cdr tid)
                                       xid (backend-command-id *backend*))
                (insert-tuple-with-unique table new
                                          :exclude-tid tid)
                (incf count))))))
      (send-command-complete stream (format nil "UPDATE ~a" count)))))
