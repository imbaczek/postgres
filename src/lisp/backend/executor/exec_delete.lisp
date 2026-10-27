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
  (exec-delete-subqueries stream ast))

(defun exec-update (stream ast)
  (exec-update-subqueries stream ast))
