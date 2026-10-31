;;;; exec_on_conflict.lisp -- unique-index arbitration and conflict updates.
(in-package #:pg)

(defun parse-on-conflict (p)
  (when (match-kw p "on")
    (expect-kw p "conflict")
    (let ((columns nil) (constraint nil) (predicate nil))
      (cond ((match-kw p "on")
             (expect-kw p "constraint")
             (setf constraint (pident p)))
            ((punct-p p #\()
             (setf columns (with-parens (p) (parse-expr-list p)))
             (when (match-kw p "where") (setf predicate (parse-expr p)))))
      (expect-kw p "do")
      (cond ((match-kw p "nothing")
             (list :action :nothing :columns columns :constraint constraint :predicate predicate))
            ((match-kw p "update")
             (unless (or columns constraint)
               (ereport "42601" "ON CONFLICT DO UPDATE requires inference specification or constraint name"))
             (expect-kw p "set")
             (let ((sets (loop collect
                               (let ((column (pident p)) (dimensions nil) (slice-p nil))
                                 (when (match-punct p #\.)
                                   (pident p)
                                   (ereport "42703" "qualified column names are not allowed in SET"))
                                 (loop while (match-punct p #\[)
                                       do (let* ((first (unless (op-p p ":") (parse-expr p)))
                                                 (colon (match-op p ":"))
                                                 (upper (if colon (unless (punct-p p #\]) (parse-expr p)) first)))
                                            (when colon (setf slice-p t))
                                            (push (list colon (if colon first nil) upper) dimensions)
                                            (expect-punct p #\])))
                                 (setf dimensions (nreverse dimensions))
                                 (expect-op p "=")
                                 (list :column column
                                       :indices (and dimensions (unless slice-p (mapcar #'third dimensions)))
                                       :slice (and slice-p
                                                   (mapcar (lambda (dimension)
                                                             (if (first dimension) (rest dimension)
                                                                 (list (list :const 1) (third dimension))))
                                                           dimensions))
                                       :expr (parse-expr p)))
                               while (match-punct p #\,))))
               (list :action :update :columns columns :constraint constraint :predicate predicate
                     :sets sets :where (when (match-kw p "where") (parse-expr p)))))
            (t (syntax-error p "expected NOTHING or UPDATE"))))))

(defun on-conflict-expression-scope (table alias)
  (let* ((target (make-base-select-scope (pg-class-name table) alias table))
         (width (length (pg-class-columns table)))
         (excluded (make-select-relation :name "excluded" :alias "excluded" :table table
                                         :identity (make-select-source-identity)
                                         :offset width :width width)))
    (make-select-scope :relations (append (select-scope-relations target) (list excluded))
                       :columns (append (select-scope-columns target) (select-relation-columns excluded))
                       :width (* 2 width))))

(defun prepare-on-conflict (table raw alias)
  (when raw
    (let* ((target-scope (make-base-select-scope (pg-class-name table) alias table))
           (indexes (unique-indexes table))
           (constraint (getf raw :constraint))
           (inference (getf raw :columns))
           (columns nil)
           (arbiters nil)
           (targets nil)
           (sets nil)
           (where-target nil)
           (expression-scope nil))
      (when constraint
        (let ((index (find constraint indexes :key #'pg-index-name :test #'string=)))
          (unless (and index (pg-index-constraint-owned-p index))
            (ereport "42704" "constraint \"~a\" for table \"~a\" does not exist"
                     constraint (pg-class-name table)))
          (setf arbiters (list (pg-index-oid index)))))
      (when inference
        (let ((simple-p t))
          (dolist (raw-expression inference)
            (let ((expression (analyze-expr raw-expression target-scope)))
              (if (eq (first expression) :col)
                  (pushnew (second expression) columns :test #'=)
                  (setf simple-p nil))))
          (when simple-p
            (setf arbiters
                  (loop for index in indexes
                        when (and (= (length columns) (length (pg-index-columns index)))
                                  (every (lambda (attnum) (member attnum columns :test #'=))
                                         (pg-index-columns index)))
                          collect (pg-index-oid index)))))
        (unless arbiters
          (ereport "42P10" "there is no unique or exclusion constraint matching the ON CONFLICT specification")))
      (unless (or constraint inference)
        (setf arbiters (mapcar #'pg-index-oid indexes)))
      (when (getf raw :predicate)
        (let ((*select-sublinks-allowed-p* nil))
          (let ((expression (coerce-boolean-expression
                             (analyze-expr (getf raw :predicate) target-scope) "ON CONFLICT")))
            (declare (ignore expression)))))
      (when (eq (getf raw :action) :update)
        (let ((scope (on-conflict-expression-scope table alias)) (groups nil))
          (setf expression-scope scope)
          (dolist (item (getf raw :sets))
            (let ((group (assoc (getf item :column) groups :test #'equal)))
              (if group (push item (cdr group))
                  (push (cons (getf item :column) (list item)) groups))))
          (dolist (group (nreverse groups))
            (let* ((column (resolve-column-or-error table (car group)))
                   (items (nreverse (cdr group)))
                   (expression
                     (cond ((every (lambda (item) (or (getf item :indices) (getf item :slice))) items)
                            (analyze-array-update-assignments items column scope (or alias (pg-class-name table))))
                           ((cdr items)
                            (ereport "42601" "multiple assignments to same column \"~a\"" (pg-column-name column)))
                           ((eq (first (getf (first items) :expr)) :default) :default)
                           (t (analyze-subquery-assignment (analyze-expr (getf (first items) :expr) scope) column))))
                   (target (list :expr expression)))
              (push target targets)
              (push (cons column target) sets)))
          (when (getf raw :where)
            (setf where-target (list :expr (coerce-boolean-expression
                                            (analyze-expr (getf raw :where) scope) "ON CONFLICT DO UPDATE WHERE")))
            (push where-target targets))))
      (list :action (getf raw :action) :arbiters arbiters
            :source-presence (if expression-scope
                                 (map 'vector #'select-relation-identity
                                      (select-scope-relations expression-scope))
                                 #())
            :constraint constraint :inference-p (not (null inference)) :inference-columns columns
            :sets (sort sets #'< :key (lambda (item) (pg-column-attnum (car item)))) :where-target where-target :targets (nreverse targets)))))

(defun refresh-on-conflict-arbiters (table conflict)
  ;; Called after relation admission under the catalog lock: refresh only index
  ;; metadata here, without analyzing or evaluating user expressions.
  (when conflict
    (let* ((indexes (unique-indexes table))
           (constraint (getf conflict :constraint))
           (columns (getf conflict :inference-columns))
           (arbiters
             (cond
               (constraint
                (let ((index (find constraint indexes :key #'pg-index-name :test #'string=)))
                  (unless (and index (pg-index-constraint-owned-p index))
                    (ereport "42704" "constraint ~a for table ~a does not exist"
                             constraint (pg-class-name table)))
                  (list (pg-index-oid index))))
               ((getf conflict :inference-p)
                (loop for index in indexes
                      when (and (= (length columns) (length (pg-index-columns index)))
                                (every (lambda (attnum) (member attnum columns :test #'=))
                                       (pg-index-columns index)))
                        collect (pg-index-oid index)))
               (t (mapcar #'pg-index-oid indexes)))))
      (when (and (getf conflict :inference-p) (null arbiters))
        (ereport "42P10" "there is no unique or exclusion constraint matching the ON CONFLICT specification"))
      (setf (getf conflict :arbiters) arbiters))))

(defun find-on-conflict-tuple (table values indexes arbiters)
  (let ((wait nil) (found nil) (xid (backend-xid *backend*)))
    (dolist (index indexes)
      (when (member (pg-index-oid index) arbiters :test #'=)
        (let ((key (index-values index values)))
          (unless (and (key-has-null-p key) (not (pg-index-nulls-not-distinct-p index)))
            (dolist (tid (btree-search-unlocked (pg-index-relfilenode index)
                                                (encode-index-key key (index-types index table))))
              (let ((tuple (heapam-get-tuple (pg-class-relfilenode table) (car tid) (cdr tid)
                                             (pg-class-columns table))))
                (when tuple
                  (let ((status (unique-conflict-status tuple xid)))
                    (cond ((eq status :violation) (unless found (setf found tuple)))
                          ((consp status) (setf wait (cdr status))))))))))))
    (cond (found (values :conflict found))
          (wait (values :wait wait))
          (t (values nil nil)))))

(defun check-on-conflict-visible (tuple)
  (when (and (eq (backend-isolation-level *backend*) :repeatable-read)
             (/= (htup-xmin tuple) (backend-xid *backend*))
             (not (htup-visible-p tuple *current-snapshot*)))
    (ereport "40001" "could not serialize access due to concurrent update")))

(defun on-conflict-update-values (table conflict old excluded)
  (subquery-update-values
   table
   (mapcar (lambda (assignment)
             (cons (car assignment) (getf (cdr assignment) :expr)))
           (getf conflict :sets))
   old (concatenate 'vector old excluded)))

(defun claim-on-conflict-candidate (table values conflict indexes)
  ;; Claim before WHERE, retaining ownership to transaction end even when WHERE
  ;; rejects the row. Visibility and cardinality errors also precede WHERE.
  (multiple-value-bind (status candidate)
      (find-on-conflict-tuple table values indexes (getf conflict :arbiters))
    (when (eq status :conflict)
      (check-on-conflict-visible candidate)
      (when (eq (getf conflict :action) :update)
        (when (and (= (htup-xmin candidate) (backend-xid *backend*))
                   (>= (htup-cid candidate) (backend-command-id *backend*)))
          (ereport "21000" "ON CONFLICT DO UPDATE command cannot affect row a second time"))
        (let ((owner (claim-on-conflict-row-lock
                      (pg-class-relfilenode table) (htup-block candidate)
                      (htup-offnum candidate) (backend-xid *backend*))))
          (when owner (return-from claim-on-conflict-candidate (values :wait owner))))))
    (values status candidate)))

(defun insert-tuple-on-conflict (table values conflict state &key compressed-reuse datum-origins)
  (declare (ignore state))
  (let ((*datum-origin-capture* t) (*query-row-origins* nil))
    (loop
      (multiple-value-bind (block off publications action tuple)
          (insert-tuple-with-unique
           table values :publication-kind :insert :compressed-reuse compressed-reuse
           :before-check
           (when conflict
             (lambda (indexes) (claim-on-conflict-candidate table values conflict indexes))))
        (declare (ignore block off))
        (unless (eq action :conflict)
          (logical-note-row-change :insert table :new-values values :publication-selection publications)
          (return (values :insert values nil)))
        (when (eq (getf conflict :action) :nothing)
          (return (values :nothing nil nil)))
        ;; The claimed row cannot change while WHERE and SET are evaluated.
        (materialize-select-tuple table tuple *current-snapshot*)
        (let* ((old (htup-values tuple))
               (environment (concatenate 'vector old values))
               (*query-row-presence* (getf conflict :source-presence))
               (*query-row-origins*
                 (concatenate 'vector
                              (or (htup-origins tuple) (make-array (length old) :initial-element nil))
                              (or datum-origins (make-array (length values) :initial-element nil))))
               (where (getf (getf conflict :where-target) :expr))
               (accepted (call-with-dml-expression-evaluation
                          (lambda () (or (null where) (true-p (eval-expr where environment)))))))
          (unless accepted (return (values :nothing nil nil)))
          (multiple-value-bind (new computed-compressed descriptors)
              (call-with-dml-expression-evaluation
               (lambda () (on-conflict-update-values table conflict old values)))
            (let ((rel (pg-class-relfilenode table))
                  (xid (backend-xid *backend*))
                  (protected-snapshot *current-snapshot*))
              ;; Refresh the claim snapshot while preserving the statement snapshot for subqueries.
              (let ((*current-snapshot* (if (eq (backend-isolation-level *backend*) :read-committed)
                                            (fresh-delete-snapshot *current-snapshot*) *current-snapshot*)))
                (multiple-value-bind (deleted actual old-block old-offnum write-status)
                    (heapam-delete-tid rel (pg-class-columns table) (htup-block tuple) (htup-offnum tuple)
                                       xid (backend-command-id *backend*))
                  (check-heapam-write-status write-status)
                  (when deleted
                    (let ((reuse (unchanged-tuple-toast-pointers actual (getf conflict :sets) table new protected-snapshot)))
                      (dml-reuse-matching-external-descriptors table actual new descriptors reuse protected-snapshot)
                      (delete-tuple-toast-values table actual xid (backend-command-id *backend*) reuse)
                      (materialize-select-tuple table actual protected-snapshot)
                      (let ((old-compressed (unchanged-tuple-compressed-values actual (getf conflict :sets) new)))
                        (dotimes (i (length computed-compressed))
                          (cond ((aref reuse i) (setf (aref computed-compressed i) nil))
                                ((and old-compressed (null (aref computed-compressed i)))
                                 (setf (aref computed-compressed i) (aref old-compressed i))))))
                      (multiple-value-bind (new-block new-offnum selection)
                          (insert-tuple-with-unique
                           table new :exclude-tid (cons old-block old-offnum) :publication-kind :update
                           :toast-reuse reuse :compressed-reuse computed-compressed
                           :on-success
                           (lambda (next-block next-offnum)
                             (unless (heapam-set-ctid-unlocked rel old-block old-offnum next-block next-offnum)
                               (ereport "XX000" "could not link updated heap tuple"))))
                        (declare (ignore new-block new-offnum))
                        (logical-note-row-change :update table :old-values (htup-values actual)
                                                 :new-values new :toast-reuse reuse :publication-selection selection))
                      (return-from insert-tuple-on-conflict (values :update new (htup-values actual))))))))))))))
