;;;; exec_insert.lisp -- INSERT with NOT NULL and unique-index enforcement.

(in-package #:pg)

(defun check-not-null (values columns)
  (loop for i from 0 below (length columns)
        for col = (aref columns i)
        when (and (pg-column-notnull col)
                  (eq (aref values i) +null+))
          do (ereport "23502"
                      "null value in column \"~a\" violates not-null constraint"
                      (pg-column-name col))))

(defvar *dml-column-defaults* nil)

(defun insert-column-default (column)
  (cond ((pg-column-default-expression column)
         (eval-expr (pg-column-default-expression column) #()))
        ((pg-column-default-sequence-name column)
         (sequence-nextval (pg-column-default-sequence-name column)))
        (t +null+)))

(defun unique-conflict-status (tuple my-xid)
  "Classify a heap tuple found in a unique index."
  (let ((xmin (htup-xmin tuple))
        (xmax (htup-xmax tuple)))
    (cond
      ;; Our own tuple: a live version conflicts, a version we already
      ;; deleted (e.g. an earlier UPDATE in this transaction) does not.
      ((and my-xid (= xmin my-xid))
       (if (= xmax my-xid) :free :violation))
      ((xid-aborted-p xmin) :free)
      ((xid-in-progress-p xmin) (cons :wait xmin))
      ((xid-committed-p xmin)
       (cond
         ((zerop xmax) :violation)
         ((and my-xid (= xmax my-xid)) :free)
         ((xid-committed-p xmax) :free)
         ((xid-in-progress-p xmax) (cons :wait xmax))
         (t :violation)))
      (t (cons :wait xmin)))))

(defun key-has-null-p (values)
  (some (lambda (v) (eq v +null+)) values))

(defun index-key-requires-uniqueness-check-p (index values &optional (unique (pg-index-unique-p index)))
  (and unique
       (or (pg-index-nulls-not-distinct-p index) (not (key-has-null-p values)))))

(defun check-unique-indexes (table indexes values exclude-tid)
  "Return :ok, (:violation . index-name), or (:wait . xid).

Default unique indexes treat NULLs as distinct; NULLS NOT DISTINCT indexes
check NULL-containing keys for conflicts too. The caller holds the B-tree
mutex for each index in INDEXES."
  (let ((wait nil)
        (violation nil)
        (my-xid (backend-xid *backend*)))
    (loop for idx in indexes when (pg-index-unique-p idx) do
      (let* ((types (index-types idx table))
             (vals (index-values idx values)))
        (when (index-key-requires-uniqueness-check-p idx vals t)
          (let ((tids (btree-search-unlocked
                       (pg-index-relfilenode idx)
                       (encode-index-key vals types))))
            (dolist (tid tids)
              (unless (and exclude-tid
                           (= (car tid) (car exclude-tid))
                           (= (cdr tid) (cdr exclude-tid)))
                (let ((tuple (heapam-get-tuple (pg-class-relfilenode table)
                                               (car tid) (cdr tid)
                                               (pg-class-columns table))))
                  (when tuple
                    (let ((status (unique-conflict-status tuple my-xid)))
                      (cond
                        ((eq status :violation)
                         (setf violation (pg-index-name idx)))
                        ((eq status :free) nil)
                        (t
                         (setf wait (cdr status)))))))))))))
    (cond (violation (cons :violation violation))
          (wait (cons :wait wait))
          (t :ok))))

(defun capture-insert-indexes (table)
  "Capture ordinary indexes, their lock nodes, and owned TOAST identity.
Take *WRITE-LOCK* only for the snapshot; the caller releases it before acquiring
B-tree relation locks, then reacquires it and checks the TOAST identity."
  (with-write-lock
    (let* ((indexes (table-indexes table nil #'pg-index-ready-p))
           (toast (pg-class-toast table))
           (toast-node (and toast (pg-toast-index-relfilenode toast))))
      (values indexes
              (append (mapcar #'pg-index-relfilenode indexes)
                      (when toast-node (list toast-node)))
              toast toast-node))))

(defun insert-tuple-with-unique (table values &key exclude-tid on-success publication-kind before-check toast-reuse compressed-reuse)
  "Insert logical VALUES, externalizing them only after uniqueness succeeds.
Index keys use the logical values; the heap receives the physical tuple. B-tree
locks precede the write lock, and transaction waits happen after both are released.
ON-SUCCESS and BEFORE-CHECK run under those locks and must not wait. BEFORE-CHECK
can claim an ON CONFLICT tuple or ask the released-lock loop to wait."
  (let* ((xid (get-current-xid))
         (relfilenode (pg-class-relfilenode table))
         (indexes nil)
         (index-relfilenodes nil)
         (toast nil)
         (toast-node nil))
    (detach-select-index-scans)
    (register-relation-writer relfilenode xid)
    (multiple-value-setq (indexes index-relfilenodes toast toast-node)
      (capture-insert-indexes table))
    (loop
      (let ((wait-xid nil))
        (with-btree-relation-locks (index-relfilenodes)
          (with-write-lock
            (ensure-current-table table)
            (unless (and (eq toast (pg-class-toast table))
                         (eql toast-node (and toast (pg-toast-index-relfilenode toast))))
              (ereport "0A000" "TOAST storage changed during INSERT"))
            (multiple-value-bind (action payload)
                (when before-check (funcall before-check indexes))
              (when (eq action :conflict)
                (return-from insert-tuple-with-unique
                  (values nil nil nil :conflict payload)))
              (let ((check (if (eq action :wait) (cons :wait payload)
                               (check-unique-indexes table indexes values exclude-tid))))
                (cond
                  ((eq check :ok)
                   (let* ((columns (pg-class-columns table))
                          (cid (backend-command-id *backend*))
                          (physical (toast-externalize-tuple-unlocked toast values columns xid cid toast-reuse compressed-reuse)))
                     (multiple-value-bind (block off)
                         (heapam-insert relfilenode columns physical xid cid)
                       (dolist (idx indexes)
                         (let ((key (index-storage-key idx table (index-values idx values) physical)))
                           (btree-insert-unlocked (pg-index-relfilenode idx) key block off)))
                       (when on-success (funcall on-success block off))
                       (return-from insert-tuple-with-unique
                         (if publication-kind
                             (values block off (logical-capture-publications (list table) publication-kind))
                             (values block off))))))
                  ((eq (car check) :violation)
                   (ereport "23505"
                            "duplicate key value violates unique constraint \"~a\""
                            (cdr check)))
                  (t (setf wait-xid (cdr check))))))))
        (when wait-xid (wait-for-xid wait-xid))))))

(defun exec-insert (stream ast)
  (call-with-observed-dml ast (lambda () (exec-insert-subqueries stream ast))))
