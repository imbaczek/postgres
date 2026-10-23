;;;; exec_analyze.lisp -- ANALYZE command entry point.

(in-package #:pg)

(defun publish-analyze-statistics (table statistics)
  (with-write-lock
    (ensure-current-table table)
    (let ((previous (pg-class-statistics table))
          (wal-generation (wal-catalog-flush-generation))
          (published-p nil))
      (unwind-protect
           (progn
             (setf (pg-class-statistics table) statistics)
             (save-catalog)
             (setf published-p t))
        (unless published-p
          ;; WAL may already contain the catalog image even if the later
          ;; catalog-file replacement failed. Keep memory aligned with that
          ;; durable recovery state, like save-catalog's generation contract.
          (unless (/= wal-generation (wal-catalog-flush-generation))
            (setf (pg-class-statistics table) previous))))))
  (values))

(defun exec-analyze (stream ast)
  (let ((table (resolve-table-or-error (getf ast :table))))
    ;; ANALYZE's table reader registration keeps DDL from replacing its heap or
    ;; column layout while analyze.c-style sampling reads the visible tuples.
    (with-registered-table (table relfilenode xid :read)
      (let ((statistics
              (analyze-table-statistics table relfilenode *current-snapshot*)))
        (publish-analyze-statistics table statistics)))
    (send-command-complete stream "ANALYZE")))
