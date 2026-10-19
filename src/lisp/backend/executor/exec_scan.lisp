;;;; exec_scan.lisp -- SELECT via sequential heap scan.

(in-package #:pg)

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

(defun exec-select (stream ast)
  (let* ((table-name (getf ast :from))
         (table (when table-name (lookup-table table-name))))
    (when (and table-name (null table))
      (ereport "42P01" "relation \"~a\" does not exist" table-name))
    (let* ((targets (analyze-targets (getf ast :targets) table))
           (where (when (getf ast :where)
                    (analyze-expr (getf ast :where) table)))
           (order (analyze-order (getf ast :order-by) table targets))
           (limit (getf ast :limit))
           (rows '()))
      (cond
        ((null table)
         (when (some (lambda (tg) (eq (first (getf tg :expr)) :col)) targets)
           (ereport "42703" "column does not exist"))
         (let ((values (map 'vector
                            (lambda (tg) (eval-expr (getf tg :expr) nil))
                            targets)))
           (when (or (null where) (true-p (eval-expr where nil)))
             (push (cons values nil) rows))))
        (t
           (let ((relfilenode (pg-class-relfilenode table))
                 (xid (get-current-xid)))
             ;; Keep DROP TABLE from removing storage during this scan or
             ;; before the surrounding transaction releases its relation use.
             (register-relation-reader relfilenode xid)
             (with-write-lock
               (ensure-current-table table))
             (heapam-scan relfilenode
                          (pg-class-columns table)
                          *current-snapshot*
                          (lambda (tuple)
                            (check-for-interrupts)
                            (let ((env (htup-values tuple)))
                              (when (or (null where) (true-p (eval-expr where env)))
                                (push (cons (map 'vector
                                                 (lambda (tg) (eval-expr (getf tg :expr) env))
                                                 targets)
                                            env)
                                      rows))))))))
      (setf rows (nreverse rows))
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
      (send-command-complete stream (format nil "SELECT ~a" (length rows))))))
