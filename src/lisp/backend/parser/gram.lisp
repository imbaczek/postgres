;;;; gram.lisp -- recursive-descent parser for the MVP SQL subset.

(in-package #:pg)

(defstruct sql-parser
  tokens
  (pos 0))

(defun pcur (p) (nth (sql-parser-pos p) (sql-parser-tokens p)))
(defun pnext (p) (prog1 (pcur p) (incf (sql-parser-pos p))))
(defun pkind (p) (token-kind (pcur p)))
(defun pval (p) (token-value (pcur p)))
(defun at-eof-p (p) (eq (pkind p) :eof))

(defun syntax-error (p &optional message)
  (let ((tok (pcur p)))
    (ereport "42601"
             (or message
                 (if (eq (token-kind tok) :eof)
                     "syntax error at end of input"
                     (format nil "syntax error at or near \"~a\""
                             (token-value tok))))
             :position (1+ (token-pos tok)))))

(defmacro define-token-matchers (kind predicate matcher expecter value
                                 &key normalize)
  "Define PREDICATE, MATCHER, and EXPECTER for one parser token kind."
  `(progn
     (defun ,predicate (p ,value)
       (and (eq (pkind p) ,kind)
            (string= (pval p)
                     ,(if normalize `(,normalize ,value) value))))
     (defun ,matcher (p ,value)
       (when (,predicate p ,value)
         (pnext p)
         t))
     (defun ,expecter (p ,value)
       (unless (,matcher p ,value)
         (syntax-error p (format nil "expected \"~a\"" ,value)))
       t)))

(define-token-matchers :ident kw-p match-kw expect-kw keyword)
(define-token-matchers :punct punct-p match-punct expect-punct ch
  :normalize string)
(define-token-matchers :op op-p match-op expect-op op)

(defmacro with-parens ((parser &key opening-consumed) &body body)
  "Evaluate BODY between required parentheses and return all its values."
  (let ((parser-value (gensym "PARSER-")))
    `(let ((,parser-value ,parser))
       ,@(unless opening-consumed
           `((expect-punct ,parser-value #\()) )
       (multiple-value-prog1
           (progn ,@body)
         (expect-punct ,parser-value #\))))))

(defun pident (p)
  (if (eq (pkind p) :ident)
      (token-value (pnext p))
      (syntax-error p "expected identifier")))

(defun parse-integer-literal (p)
  (if (eq (pkind p) :integer)
      (token-value (pnext p))
      (syntax-error p "expected integer literal")))

(defun clause-keyword-p (word)
  (member word
          '("from" "where" "order" "limit" "group" "having" "union"
            "intersect" "except" "offset" "fetch" "for" "join" "inner"
            "left" "right" "full" "cross" "on" "using" "asc" "desc"
            "and" "or")
          :test #'string=))

;;; ---- statements ----

(defun parse-statements (sql)
  (let* ((tokens (tokenize sql))
         (p (make-sql-parser :tokens tokens))
         (stmts '()))
    (loop
      (loop while (match-punct p #\;))
      (when (at-eof-p p)
        (return (nreverse stmts)))
      (push (parse-statement p) stmts)
      (cond
        ((match-punct p #\;))
        ((at-eof-p p)
         (return (nreverse stmts)))
        (t
         (syntax-error p "expected \";\" or end of input"))))))

(defun parse-statement (p)
  (cond
    ((match-kw p "select") (parse-select p))
    ((match-kw p "insert") (parse-insert p))
    ((match-kw p "update") (parse-update p))
    ((match-kw p "delete") (parse-delete p))
    ((match-kw p "vacuum") (parse-vacuum p))
    ((match-kw p "create") (parse-create p))
    ((match-kw p "drop") (parse-drop p))
    ((match-kw p "alter") (parse-alter p))
    ((match-kw p "begin") (match-kw p "work") (match-kw p "transaction")
                          (list :type :begin))
    ((match-kw p "start") (expect-kw p "transaction") (list :type :begin))
    ((match-kw p "commit") (match-kw p "work") (match-kw p "transaction")
                           (list :type :commit))
    ((match-kw p "end") (match-kw p "work") (match-kw p "transaction")
                        (list :type :commit))
    ((match-kw p "rollback") (match-kw p "work") (match-kw p "transaction")
                             (list :type :rollback))
    ((match-kw p "abort") (match-kw p "work") (match-kw p "transaction")
                          (list :type :rollback))
    ((match-kw p "checkpoint") (list :type :checkpoint))
    ((match-kw p "set") (parse-set p))
    ((match-kw p "show") (parse-show p))
    ((match-kw p "reset") (parse-reset p))
    (t
     (ereport "42601" "syntax error at or near \"~a\"" (pval p)))))

(defun parse-vacuum (p)
  (list :type :vacuum :table (strip-schema (pident p))))

;;; ---- SELECT ----

(defun parse-select (p)
  (when (match-kw p "distinct")
    (ereport "0A000" "SELECT DISTINCT is not supported"))
  (match-kw p "all")
  (let ((targets (parse-target-list p))
        (from nil)
        (from-alias nil)
        (where nil)
        (order-by nil)
        (limit nil))
    (when (match-kw p "from")
      (multiple-value-setq (from from-alias) (parse-from p)))
    (when (match-kw p "where")
      (setf where (parse-expr p)))
    (when (match-kw p "order")
      (expect-kw p "by")
      (setf order-by (parse-order-list p)))
    (when (match-kw p "limit")
      (setf limit (parse-integer-literal p)))
    (when (kw-p p "group")
      (ereport "0A000" "GROUP BY is not supported"))
    (when (kw-p p "having")
      (ereport "0A000" "HAVING is not supported"))
    (list :type :select
          :targets targets
          :from from
          :from-alias from-alias
          :where where
          :order-by order-by
          :limit limit)))

(defun parse-target-list (p)
  (if (match-op p "*")
      (list (list :star t))
      (loop collect (parse-target p)
            while (match-punct p #\,))))

(defun parse-target (p)
  (let ((expr (parse-expr p))
        (alias nil))
    (cond
      ((match-kw p "as")
       (setf alias (pident p)))
      ((and (eq (pkind p) :ident)
            (not (clause-keyword-p (pval p))))
       (setf alias (pident p))))
    (list :expr expr :alias alias)))

(defun parse-from (p)
  (let ((table (pident p))
        (alias nil))
    (when (match-kw p "as")
      (setf alias (pident p)))
    (when (and (eq (pkind p) :ident)
               (not (clause-keyword-p (pval p))))
      (setf alias (pident p)))
    (when (or (punct-p p #\,)
              (kw-p p "join") (kw-p p "inner") (kw-p p "left")
              (kw-p p "right") (kw-p p "full") (kw-p p "cross"))
      (ereport "0A000" "joins are not supported"))
    (values (strip-schema table) alias)))

(defun parse-order-list (p)
  (loop collect (let ((expr (parse-expr p))
                      (desc nil))
                  (cond ((match-kw p "desc") (setf desc t))
                        ((match-kw p "asc") nil))
                  (list :expr expr :desc desc))
        while (match-punct p #\,)))

;;; ---- INSERT / UPDATE / DELETE ----

(defun parse-insert (p)
  (expect-kw p "into")
  (let ((table (strip-schema (pident p)))
        (columns nil))
    (when (match-punct p #\()
      (setf columns
            (with-parens (p :opening-consumed t)
              (parse-ident-list p))))
    (expect-kw p "values")
    (let ((rows (loop collect (parse-values-row p)
                      while (match-punct p #\,))))
      (list :type :insert :table table :columns columns :rows rows))))

(defun parse-values-row (p)
  (with-parens (p)
    (parse-expr-list p)))

(defun parse-ident-list (p)
  (loop collect (pident p) while (match-punct p #\,)))

(defun parse-expr-list (p)
  (loop collect (parse-expr p) while (match-punct p #\,)))

(defun parse-update (p)
  (let ((table (strip-schema (pident p))))
    (expect-kw p "set")
    (let ((sets (loop collect (let ((col (pident p)))
                                (expect-op p "=")
                                (list :column col :expr (parse-expr p)))
                      while (match-punct p #\,)))
          (where nil))
      (when (match-kw p "where")
        (setf where (parse-expr p)))
      (list :type :update :table table :sets sets :where where))))

(defun parse-delete (p)
  (expect-kw p "from")
  (let ((table (strip-schema (pident p)))
        (where nil))
    (when (match-kw p "where")
      (setf where (parse-expr p)))
    (list :type :delete :table table :where where)))

;;; ---- CREATE / DROP / ALTER ----

(defun parse-create (p)
  (cond
    ((match-kw p "table") (parse-create-table p))
    ((match-kw p "unique")
     (expect-kw p "index")
     (parse-create-index p t))
    ((match-kw p "index") (parse-create-index p nil))
    (t
     (ereport "0A000" "unsupported CREATE statement"))))

(defun parse-create-table (p)
  (let ((if-not-exists
          (when (match-kw p "if")
            (expect-kw p "not")
            (expect-kw p "exists")
            t))
        (name (strip-schema (pident p)))
        (columns '())
        (constraints '()))
    (expect-punct p #\()
    (loop
      (if (or (kw-p p "primary") (kw-p p "unique") (kw-p p "constraint"))
          (push (parse-table-constraint p) constraints)
          (push (parse-column-def p) columns))
      (unless (match-punct p #\,)
        (return)))
    (expect-punct p #\))
    (list :type :create-table
          :name name
          :if-not-exists if-not-exists
          :columns (nreverse columns)
          :constraints (nreverse constraints))))

(defun parse-column-def (p)
  (let* ((name (pident p))
         (type nil)
         (typmod -1))
    (multiple-value-setq (type typmod) (parse-type p))
    (let ((notnull nil)
          (primary nil)
          (unique nil)
          (nulls-not-distinct nil))
      (loop
        (cond
          ((match-kw p "not")
           (expect-kw p "null")
           (setf notnull t))
          ((match-kw p "null") nil)
          ((match-kw p "primary")
           (expect-kw p "key")
           (setf primary t notnull t))
          ((match-kw p "unique")
           (setf unique t
                 nulls-not-distinct (parse-unique-null-treatment p)))
          ((match-kw p "default")
           (ereport "0A000" "DEFAULT is not supported"))
          ((match-kw p "check")
           (ereport "0A000" "CHECK is not supported"))
          ((match-kw p "references")
           (ereport "0A000" "foreign keys are not supported"))
          (t (return))))
      (list :name name :type type :typmod typmod
            :notnull notnull :primary primary :unique unique
            :nulls-not-distinct nulls-not-distinct))))

(defun parse-type (p)
  (cond
    ((match-kw p "int") (values :int4 -1))
    ((match-kw p "integer") (values :int4 -1))
    ((match-kw p "int4") (values :int4 -1))
    ((match-kw p "bigint") (values :int8 -1))
    ((match-kw p "int8") (values :int8 -1))
    ((match-kw p "varchar") (values :varchar (parse-varchar-typmod p)))
    ((match-kw p "character")
     (expect-kw p "varying")
     (values :varchar (parse-varchar-typmod p)))
    ((match-kw p "text")
     ;; Temporary compatibility alias: text currently has varchar(126)
     ;; storage and length semantics.  Keep the real type distinction as a
     ;; TODO; PostgreSQL assigns text a distinct type OID and no typmod.
     (values :varchar (+ 126 4)))
    ((match-kw p "serial")
     (ereport "0A000" "type serial is not supported"))
    (t
     (ereport "0A000" "unsupported type \"~a\"" (pval p)))))

(defun parse-varchar-typmod (p)
  (let ((n (if (match-punct p #\()
               (with-parens (p :opening-consumed t)
                 (parse-integer-literal p))
               126)))
    (when (> n 126)
      (ereport "0A000" "varchar length greater than 126 is not supported"))
    (when (< n 1)
      (ereport "22023" "length for type varchar must be at least 1"))
    (+ n 4)))

(defun parse-unique-null-treatment (p)
  "Parse optional NULLS [NOT] DISTINCT; return value and whether specified."
  (if (match-kw p "nulls")
      (let ((not-distinct (match-kw p "not")))
        (expect-kw p "distinct")
        (values not-distinct t))
      (values nil nil)))

(defun parse-table-constraint (p)
  (let ((cname nil))
    (when (match-kw p "constraint")
      (setf cname (pident p)))
    (cond
      ((match-kw p "primary")
       (expect-kw p "key")
       (list :kind :primary :name cname
             :columns (with-parens (p) (parse-ident-list p))))
      ((match-kw p "unique")
       (let ((nulls-not-distinct (parse-unique-null-treatment p)))
         (list :kind :unique :name cname
               :columns (with-parens (p) (parse-ident-list p))
               :nulls-not-distinct nulls-not-distinct)))
      (t
       (ereport "0A000" "unsupported table constraint")))))

(defun parse-create-index (p unique)
  (let ((if-not-exists
          (when (match-kw p "if")
            (expect-kw p "not")
            (expect-kw p "exists")
            t))
        (name (strip-schema (pident p))))
    (expect-kw p "on")
    (let ((table (strip-schema (pident p))))
      (when (match-kw p "using")
        (unless (match-kw p "btree")
          (ereport "0A000" "only btree indexes are supported")))
      (expect-punct p #\()
      (let ((cols '()))
        (loop
          (push (pident p) cols)
          (when (match-kw p "asc") nil)
          (when (match-kw p "desc")
            (ereport "0A000" "DESC indexes are not supported"))
          (unless (match-punct p #\,)
            (return)))
        (expect-punct p #\))
        (let ((nulls-not-distinct
                (if unique
                    (parse-unique-null-treatment p)
                    (when (kw-p p "nulls")
                      (ereport "0A000"
                               "NULLS NOT DISTINCT requires a unique index")))))
          (when (match-kw p "where")
            (ereport "0A000" "partial indexes are not supported"))
          (list :type :create-index
                :name name
                :table table
                :columns (nreverse cols)
                :unique unique
                :nulls-not-distinct nulls-not-distinct
                :if-not-exists if-not-exists))))))

(defun parse-drop (p)
  (cond
    ((match-kw p "table")
     (let ((if-exists (when (match-kw p "if")
                        (expect-kw p "exists")
                        t)))
       (let ((names (loop collect (strip-schema (pident p))
                          while (match-punct p #\,))))
         (match-kw p "cascade")
         (match-kw p "restrict")
         (list :type :drop-table :names names :if-exists if-exists))))
    ((match-kw p "index")
     (let ((if-exists (when (match-kw p "if")
                        (expect-kw p "exists")
                        t)))
       (let ((names (loop collect (strip-schema (pident p))
                          while (match-punct p #\,))))
         (match-kw p "cascade")
         (match-kw p "restrict")
         (list :type :drop-index :names names :if-exists if-exists))))
    (t
     (ereport "0A000" "unsupported DROP statement"))))

(defun parse-alter (p)
  (cond
    ((match-kw p "index")
     (let ((name (strip-schema (pident p))))
       (expect-kw p "rename")
       (expect-kw p "to")
       (list :type :alter-index :name name :new-name (pident p))))
    ((match-kw p "table")
     (parse-alter-table p))
    (t
     (syntax-error p "expected TABLE or INDEX after ALTER"))))

(defun parse-alter-table (p)
  (let ((if-exists (when (match-kw p "if")
                     (expect-kw p "exists")
                     t))
        (table (strip-schema (pident p))))
    (let ((actions (list (parse-alter-table-action p))))
      (loop while (match-punct p #\,)
            do (push (parse-alter-table-action p) actions))
      (if (cdr actions)
          (list :type :alter-table :table table :if-exists if-exists
                :actions (nreverse actions))
          (append (list :type :alter-table :table table :if-exists if-exists)
                  (first actions))))))

(defun parse-alter-table-action (p)
  (cond
    ((match-kw p "rename")
     (expect-kw p "constraint")
     (let ((constraint-name (pident p)))
       (expect-kw p "to")
       (list :action :rename-constraint
             :constraint-name constraint-name
             :new-name (pident p))))
    ((match-kw p "drop")
     (expect-kw p "constraint")
     (let ((constraint-if-exists (when (match-kw p "if")
                                   (expect-kw p "exists")
                                   t))
           (constraint-name (pident p)))
       (match-kw p "restrict")
       (list
        :constraint-if-exists constraint-if-exists
        :action :drop-constraint
        :constraint-name constraint-name)))
    ((match-kw p "add")
     (let ((cname nil))
       (when (match-kw p "constraint")
         (setf cname (pident p)))
       (cond
         ((match-kw p "primary")
          (expect-kw p "key")
          (if (match-kw p "using")
              (progn
                (expect-kw p "index")
                (list :action :add-primary-key
                      :using-index (strip-schema (pident p))
                      :constraint-name cname))
              (list :action :add-primary-key
                    :columns (with-parens (p) (parse-ident-list p))
                    :constraint-name cname)))
         ((match-kw p "unique")
          (multiple-value-bind (nulls-not-distinct null-treatment-specified)
              (parse-unique-null-treatment p)
            (when (and null-treatment-specified (kw-p p "using"))
              (syntax-error p))
            (if (match-kw p "using")
                (progn
                  (expect-kw p "index")
                  (list :action :add-unique
                        :using-index (strip-schema (pident p))
                        :constraint-name cname
                        :nulls-not-distinct nulls-not-distinct))
                (list :action :add-unique
                      :columns (with-parens (p) (parse-ident-list p))
                      :constraint-name cname
                      :nulls-not-distinct nulls-not-distinct))))
         (t
          (ereport "0A000" "unsupported ALTER TABLE action")))))
    (t
     (ereport "0A000" "unsupported ALTER TABLE action"))))

;;; ---- SET / SHOW / RESET ----

(defun parse-isolation-level (p)
  (expect-kw p "isolation")
  (expect-kw p "level")
  (cond
    ((match-kw p "read")
     (cond
       ((match-kw p "committed") nil)
       ((match-kw p "uncommitted")
        (ereport "0A000" "read uncommitted is not supported"))
       (t (syntax-error p))))
    ((match-kw p "repeatable")
     (expect-kw p "read")
     (ereport "0A000" "repeatable read is not supported"))
    ((match-kw p "serializable")
     (ereport "0A000" "serializable is not supported"))
    (t (syntax-error p)))
  nil)

(defun parse-set-value (p)
  (cond
    ((eq (pkind p) :integer)
     (princ-to-string (token-value (pnext p))))
    ((eq (pkind p) :string)
     (token-value (pnext p)))
    ((eq (pkind p) :ident)
     (if (kw-p p "default")
         (progn (pnext p) :default)
         (pident p)))
    (t (syntax-error p "expected setting value"))))

(defun parse-set (p)
  (when (match-kw p "session") nil)
  (when (match-kw p "local")
    (ereport "0A000" "SET LOCAL is not supported"))
  (cond
    ((match-kw p "transaction")
     (cond
       ((kw-p p "isolation")
        (parse-isolation-level p))
       ((match-kw p "read")
        (cond ((match-kw p "only")
               (ereport "0A000" "SET TRANSACTION READ ONLY is not supported"))
              ((match-kw p "write") nil)
              (t (syntax-error p))))
       (t (syntax-error p)))
     (list :type :set :name "transaction_isolation" :value "read committed"))
    ((match-kw p "time")
     (expect-kw p "zone")
     (list :type :set :name "timezone" :value (parse-set-value p)))
    ((match-kw p "characteristics")
     (expect-kw p "as")
     (expect-kw p "transaction")
     (parse-isolation-level p)
     (list :type :set :name "transaction_isolation" :value "read committed"))
    (t
     (let ((name (pident p)))
       (when (eq (pkind p) :op)
         (pnext p))
       (match-kw p "to")
       (list :type :set :name name :value (parse-set-value p))))))

(defun parse-show (p)
  (let ((name (pident p)))
    (when (string= name "transaction")
      (expect-kw p "isolation")
      (expect-kw p "level")
      (setf name "transaction_isolation"))
    (list :type :show :name name)))

(defun parse-reset (p)
  (cond
    ((match-kw p "all")
     (list :type :reset :name "all"))
    ((match-kw p "time")
     (expect-kw p "zone")
     (list :type :reset :name "timezone"))
    (t
     (list :type :reset :name (pident p)))))

;;; ---- expressions ----

(defun parse-expr (p) (parse-or p))

(defun parse-or (p)
  (let ((left (parse-and p)))
    (loop while (match-kw p "or")
          do (setf left (list :or left (parse-and p))))
    left))

(defun parse-and (p)
  (let ((left (parse-not p)))
    (loop while (match-kw p "and")
          do (setf left (list :and left (parse-not p))))
    left))

(defun parse-not (p)
  (if (match-kw p "not")
      (list :not (parse-not p))
      (parse-comparison p)))

(defun parse-comparison (p)
  (let ((left (parse-additive p)))
    (cond
      ((and (eq (pkind p) :op)
            (member (pval p) '("=" "<>" "!=" "<" "<=" ">" ">=")
                    :test #'string=))
       (let ((op (pval p)))
         (pnext p)
         (list :binop (if (string= op "!=") "<>" op)
               left
               (parse-additive p))))
      ((match-kw p "is")
       (let ((negated (match-kw p "not")))
         (cond
           ((match-kw p "null") (list :is-null left negated))
           ((match-kw p "true") (list :is-bool left negated t))
           ((match-kw p "false") (list :is-bool left negated nil))
           (t (syntax-error p "expected NULL, TRUE or FALSE after IS")))))
      (t left))))

(defun parse-binary-level (p ops next)
  (let ((left (funcall next p)))
    (loop
      (unless (and (eq (pkind p) :op)
                   (member (pval p) ops :test #'string=))
        (return left))
      (let ((op (pval p)))
        (pnext p)
        (setf left (list :binop op left (funcall next p)))))))

(defun parse-additive (p)
  (parse-binary-level p '("+" "-") #'parse-multiplicative))

(defun parse-multiplicative (p)
  (parse-binary-level p '("*" "/" "%") #'parse-unary))

(defun parse-unary (p)
  (cond
    ((match-op p "-") (list :unary "-" (parse-unary p)))
    ((match-op p "+") (parse-unary p))
    (t (parse-primary p))))

(defun parse-primary (p)
  (cond
    ((eq (pkind p) :integer)
     (list :const (token-value (pnext p))))
    ((eq (pkind p) :string)
     (list :const (token-value (pnext p))))
    ((match-punct p #\()
     (with-parens (p :opening-consumed t)
       (parse-expr p)))
    ((match-kw p "null") (list :const +null+))
    ((match-kw p "true") (list :bool t))
    ((match-kw p "false") (list :bool nil))
    ((eq (pkind p) :ident)
     (let ((name (pident p)))
       (cond
         ((match-punct p #\.)
          (let ((col (pident p)))
            (list :column name col)))
         ((match-punct p #\()
          (let ((args (if (match-punct p #\))
                          '()
                          (with-parens (p :opening-consumed t)
                            (parse-expr-list p)))))
            (list :func name args)))
         ((member name '("current_user" "session_user" "user"
                         "current_database" "current_schema" "current_catalog")
                  :test #'string=)
          (list :func name '()))
         (t
          (list :column name)))))
    (t
     (syntax-error p))))
