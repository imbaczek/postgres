;;;; exec_insert.lisp -- INSERT with NOT NULL and unique-index enforcement.

(in-package #:pg)

(defun coerce-value (value type typmod)
  (cond
    ((eq value +null+) +null+)
    ((eq type :int4)
     (let ((n (cond
                ((integerp value) value)
                ((stringp value)
                 (handler-case (parse-integer value :junk-allowed nil)
                   (error (e)
                     (if (fatal-error-p e)
                         (error e)
                         (ereport "22P02"
                                  "invalid input syntax for type integer: \"~a\""
                                  value)))))
                (t (ereport "22P02" "invalid input syntax for type integer")))))
       (unless (<= +int4-min+ n +int4-max+)
         (ereport "22003" "integer out of range"))
       n))
    ((eq type :int8)
     (let ((n (cond
                ((integerp value) value)
                ((stringp value)
                 (handler-case (parse-integer value :junk-allowed nil)
                   (error (e)
                     (if (fatal-error-p e)
                         (error e)
                         (ereport "22P02"
                                  "invalid input syntax for type bigint: \"~a\""
                                  value)))))
                (t (ereport "22P02" "invalid input syntax for type bigint")))))
       (unless (<= +int8-min+ n +int8-max+)
         (ereport "22003" "bigint out of range"))
       n))
    ((eq type :varchar)
     (let ((s (if (stringp value) value (princ-to-string value)))
           (maxlen (- typmod 4)))
       (when (> (length s) maxlen)
         (ereport "22001"
                  "value too long for type character varying(~a)"
                  maxlen))
       s))
    (t value)))

(defun check-not-null (values columns)
  (loop for i from 0 below (length columns)
        for col = (aref columns i)
        when (and (pg-column-notnull col)
                  (eq (aref values i) +null+))
          do (ereport "23502"
                      "null value in column \"~a\" violates not-null constraint"
                      (pg-column-name col))))

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

(defun check-unique-indexes (table values exclude-tid)
  "Return :ok, (:violation . index-name), or (:wait . xid).

Default unique indexes treat NULLs as distinct; NULLS NOT DISTINCT indexes
check NULL-containing keys for conflicts too."
  (let ((wait nil)
        (violation nil)
        (my-xid (backend-xid *backend*)))
    (dolist (idx (unique-indexes table))
      (let* ((types (index-types idx table))
             (vals (index-values idx values)))
        (unless (and (key-has-null-p vals)
                     (not (pg-index-nulls-not-distinct-p idx)))
          (let ((tids (btree-search (pg-index-relfilenode idx)
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

(defun insert-tuple-with-unique (table values &key exclude-tid)
  (let* ((xid (get-current-xid))
         (relfilenode (pg-class-relfilenode table)))
    (register-relation-writer relfilenode xid)
    (loop
      (let ((wait-xid nil))
        (with-write-lock
          (ensure-current-table table)
          (let ((check (check-unique-indexes table values exclude-tid)))
            (cond
              ((eq check :ok)
               (let ((columns (pg-class-columns table)))
                 (multiple-value-bind (block off)
                     (heapam-insert relfilenode columns values xid
                                    (backend-command-id *backend*)
                                    :allow-reuse (null (pg-class-indexes table)))
                   (dolist (idx (table-indexes table))
                     (let* ((types (index-types idx table))
                            (key (encode-index-key
                                  (index-values idx values)
                                  types)))
                       (btree-insert (pg-index-relfilenode idx)
                                     key block off)))
                   (return-from insert-tuple-with-unique
                     (values block off)))))
              ((eq (car check) :violation)
               (ereport "23505"
                        "duplicate key value violates unique constraint \"~a\""
                        (cdr check)))
              (t
               (setf wait-xid (cdr check))))))
        (when wait-xid
          (wait-for-xid wait-xid))))))

(defun exec-insert (stream ast)
  (let ((table (resolve-table-or-error (getf ast :table))))
    (let* ((columns (pg-class-columns table))
           (names (getf ast :columns))
           (attnums
             (if names
                 (progn
                   (when (/= (length names)
                             (length (remove-duplicates names :test #'string=)))
                     (ereport "42701" "column specified more than once"))
                   (mapcar (lambda (n)
                             (pg-column-attnum (resolve-column-or-error table n)))
                           names))
                 (loop for i from 1 to (length columns) collect i)))
           (count 0))
      (dolist (row (getf ast :rows))
        (check-for-interrupts)
        (unless (= (length row) (length attnums))
          (ereport "42601"
                   "INSERT has more target columns than expressions"))
        (let ((values (make-array (length columns) :initial-element +null+)))
          (loop for expr in row
                for attnum in attnums
                for col = (aref columns (1- attnum))
                do (setf (aref values (1- attnum))
                         (coerce-value
                          (eval-expr (analyze-expr expr nil) nil)
                          (pg-column-type col)
                          (pg-column-typmod col))))
          (check-not-null values columns)
          (insert-tuple-with-unique table values)
          (incf count)))
      (send-command-complete stream (format nil "INSERT 0 ~a" count)))))
