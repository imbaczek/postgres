;;;; gram.lisp -- recursive-descent parser for the MVP SQL subset.

(in-package #:pg)

(defstruct sql-parser
  tokens
  (pos 0))

(defvar *parse-type-modifiers-p* nil
  "Preserve unary plus when parsing type modifier expressions.")

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
            ,@(when (eq kind :ident) '((not (token-quoted-p (pcur p)))))
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

(declaim (ftype function parse-apply-logical-event))

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
  (when (eq (pkind p) :numeric) (ereport "0A000" "decimal numeric literals are not supported"))
  (if (eq (pkind p) :integer)
      (token-value (pnext p))
      (syntax-error p "expected integer literal")))

(defun parse-string-literal (p)
  (if (eq (pkind p) :string)
      (token-value (pnext p))
      (syntax-error p "expected string literal")))

(defun parse-signed-integer-literal (p)
  (let ((negative-p (match-op p "-")))
    (* (if negative-p -1 1) (parse-integer-literal p))))

(defun clause-keyword-p (word)
  (member word
          '("with" "from" "where" "order" "limit" "group" "having" "union"
            "intersect" "except" "offset" "fetch" "for" "join" "inner"
            "left" "right" "full" "outer" "cross" "on" "using"
            "natural" "asc" "desc" "window" "lateral" "returning" "conflict"
            "and" "or" "when" "then" "else" "end")
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
    ((match-kw p "explain") (parse-explain p))
    ((kw-p p "with") (parse-with-select p))
    ((match-kw p "select") (parse-select p))
    ((punct-p p #\() (parse-select-expression p))
    ((match-kw p "copy") (parse-copy-to p))
    ((match-kw p "insert") (parse-insert p))
    ((match-kw p "update") (parse-update p))
    ((match-kw p "delete") (parse-delete p))
    ((match-kw p "truncate") (parse-truncate p))
    ((match-kw p "vacuum") (parse-vacuum p))
    ((match-kw p "analyze") (parse-analyze p))
    ((match-kw p "create") (parse-create p))
    ((match-kw p "drop") (parse-drop p))
    ((match-kw p "alter") (parse-alter p))
    ((match-kw p "apply") (parse-apply-logical-event p))
    ((match-kw p "begin")
     (match-kw p "work")
     (match-kw p "transaction")
     (append (list :type :begin) (parse-transaction-characteristics p)))
    ((match-kw p "start")
     (expect-kw p "transaction")
     (append (list :type :begin) (parse-transaction-characteristics p)))
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

(defun parse-apply-logical-event (p)
  ;; Private worker command; exec-apply-logical-event checks the connection's
  ;; application name before accepting this trusted replication payload.
  (expect-kw p "logical")
  (expect-kw p "event")
  (let ((event-key (parse-string-literal p))
        (event-lsn (parse-integer-literal p))
        (payload (parse-string-literal p)))
    (list :type :apply-logical-event :event-key event-key
          :event-lsn event-lsn :payload payload)))

(defun parse-vacuum (p)
  (let ((full-p nil))
    (cond
      ((match-kw p "full")
       (setf full-p t))
      ((match-punct p #\()
       (unless (match-kw p "full")
         (syntax-error p "expected \"FULL\""))
       (expect-punct p #\))
       (setf full-p t)))
    (list :type :vacuum :full-p full-p
          :table (parse-mutation-relation-identifier p))))

(defun parse-analyze (p)
  (list :type :analyze :table (parse-mutation-relation-identifier p)))

(defun parse-explain (p)
  (let ((analyze-p (match-kw p "analyze")))
    (when (match-punct p #\()
      (unless (match-kw p "analyze")
        (ereport "0A000" "EXPLAIN currently supports only the ANALYZE option"))
      (expect-punct p #\))
      (setf analyze-p t))
    (if (or (kw-p p "select") (kw-p p "with") (punct-p p #\())
        (list :type :explain :analyze-p analyze-p
              :statement (parse-select-expression p))
        (ereport "0A000" "EXPLAIN currently supports SELECT only"))))

;;; ---- SELECT ----

(defun parse-select-core (p)
  (let ((distinct (match-kw p "distinct")))
    (when (and distinct (kw-p p "on"))
      (ereport "0A000" "SELECT DISTINCT ON is not supported"))
    (unless distinct (match-kw p "all"))
    (let ((targets (parse-target-list p))
          (from nil) (from-alias nil) (joins nil) (where nil) (group-by nil) (having nil))
      (when (match-kw p "from")
        (multiple-value-setq (from from-alias joins) (parse-from p)))
      (when (match-kw p "where") (setf where (parse-expr p)))
      (when (match-kw p "group")
        (expect-kw p "by")
        (setf group-by (parse-expr-list p)))
      (when (match-kw p "having") (setf having (parse-expr p)))
      (list :type :select :targets targets :from from :from-alias from-alias
            :joins joins :where where :group-by group-by :having having :distinct distinct
            :windows (when (match-kw p "window") (parse-window-clause p))))))

(defun parse-select-operand (p)
  (if (match-punct p #\()
      (with-parens (p :opening-consumed t) (parse-select-expression p))
      (progn (expect-kw p "select") (parse-select-core p))))

(defun parse-select-expression (p)
  (let ((query (if (kw-p p "with") (parse-with-select p)
                   (parse-select-tail p (parse-select-operand p)))))
    (unless (eq (getf query :type) :select)
      (ereport "0A000" "data-modifying statements are not allowed in a SELECT subquery"))
    query))

(defun parse-select-tail (p left)
  "Set operations bind more tightly than an unparenthesized ORDER BY or LIMIT."
  (loop while (match-kw p "union")
        do (let ((all-p (match-kw p "all")))
             (unless all-p (match-kw p "distinct"))
             (setf left (list :type :select :set-operation (if all-p :union-all :union)
                              :left left :right (parse-select-operand p)))))
  (when (or (kw-p p "intersect") (kw-p p "except"))
    (ereport "0A000" "INTERSECT and EXCEPT are not supported"))
  (when (match-kw p "order")
    (when (getf left :order-by) (syntax-error p "multiple ORDER BY clauses"))
    (expect-kw p "by")
    (setf (getf left :order-by) (parse-order-list p)))
  (when (match-kw p "limit")
    (when (getf left :limit) (syntax-error p "multiple LIMIT clauses"))
    (setf (getf left :limit) (parse-integer-literal p)))
  left)

(defun parse-select (p)
  (parse-select-tail p (parse-select-core p)))

(defun parse-qualified-target-star (p)
  (when (eq (pkind p) :ident)
    (let* ((probe (copy-sql-parser p))
           (qualifier (pident probe)))
      (when (and (match-punct probe #\.) (match-op probe "*"))
        (pident p)
        (expect-punct p #\.)
        (expect-op p "*")
        (list :qualified-star qualifier)))))

(defun parse-returning-clause (p)
  (when (match-kw p "returning")
    (let ((options nil))
      (when (match-kw p "with")
        (setf options
              (with-parens (p)
                (let ((result nil))
                  (loop
                    (let ((kind (cond ((match-kw p "old") :old)
                                      ((match-kw p "new") :new)
                                      (t (syntax-error p "expected OLD or NEW")))))
                      (when (getf result kind)
                        (ereport "42601" "~a cannot be specified multiple times" kind))
                      (expect-kw p "as")
                      (setf result (append result (list kind (pident p)))))
                    (unless (match-punct p #\,) (return result)))))))
      (values (parse-target-list p) options))))

(defun parse-target-list (p)
  (loop collect (or (when (match-op p "*") (list :star t))
                    (parse-qualified-target-star p)
                    (parse-target p))
        while (match-punct p #\,)))

(defun parse-target (p)
  (let ((expr (parse-expr p))
        (alias nil))
    (cond
      ((match-kw p "as")
       (setf alias (pident p)))
      ((and (eq (pkind p) :ident)
            (or (token-quoted-p (pcur p))
                (and (not (string= (pval p) "filter"))
                     (not (clause-keyword-p (pval p))))))
       (setf alias (pident p))))
    (list :expr expr :alias alias)))

(defun parse-mutation-relation-identifier (p)
  "Reject unsupported dotted literal names before legacy mutation name handling."
  (let ((name (pident p)))
    (when (find #\. name)
      (ereport "0A000" "dotted literal relation identifiers are not supported for mutations"))
    name))

(defun parse-from-item (p)
  (let ((lateral (match-kw p "lateral")))
    (when (match-punct p #\()
      (return-from parse-from-item
        (with-parens (p :opening-consumed t)
          (let ((query (parse-select-expression p)))
            (setf (getf query :lateral) lateral)
            (make-sql-derived-relation :query query)))))
    (let* ((name (pident p))
           (schema (when (match-punct p #\.)
                     (prog1 name (setf name (pident p))))))
      (if (match-punct p #\()
          (let ((call (parse-function-call p name schema)))
            (values (make-sql-derived-relation
                     :query (list :type :select :lateral t :from-function t
                                  :targets (list (list :expr (list :record-expand call :once t)))))
                    name))
          (if lateral (syntax-error p "LATERAL requires a subquery or function")
              (if schema (make-sql-relation-name :schema schema :name name) name))))))

(defun parse-from-relation (p)
  (multiple-value-bind (source default-alias) (parse-from-item p)
    (let ((alias default-alias))
      (if (match-kw p "as")
          (setf alias (pident p))
          (when (and (eq (pkind p) :ident)
                     (or (token-quoted-p (pcur p)) (not (clause-keyword-p (pval p)))))
            (setf alias (pident p))))
      (when (and alias (match-punct p #\())
        (unless (sql-derived-relation-p source)
          (ereport "0A000" "column aliases for base tables are not supported"))
        (setf (getf (sql-derived-relation-query source) :column-aliases)
              (with-parens (p :opening-consumed t) (parse-ident-list p))))
      (values source alias))))

(defun parse-join-clause (p)
  (let ((join-type :inner)
        (join-consumed nil))
    (cond
      ((match-kw p "join")
       (setf join-consumed t))
      ((match-kw p "inner")
       (setf join-type :inner))
      ((match-kw p "left")
       (setf join-type :left))
      ((match-kw p "right")
       (setf join-type :right))
      ((match-kw p "full")
       (setf join-type :full))
      ((match-kw p "cross")
       (setf join-type :cross))
      (t (syntax-error p "expected JOIN")))
    (unless join-consumed
      (when (member join-type '(:left :right :full))
        (match-kw p "outer"))
      (expect-kw p "join"))
    (multiple-value-bind (table alias) (parse-from-relation p)
      (let ((join (list :type join-type :table table :alias alias)))
        (cond
          ((eq join-type :cross)
           (list* :condition :cross join))
          ((match-kw p "on")
           (list* :condition (parse-expr p) join))
          ((match-kw p "using")
           (list* :condition (with-parens (p) (parse-ident-list p))
                  :using t join))
          (t
           (syntax-error p "expected ON or USING after JOIN")))))))

(defun parse-from (p)
  (multiple-value-bind (table alias) (parse-from-relation p)
    (let ((joins nil) (comma-p nil) (explicit-p nil))
      (loop
        (cond
          ((or (kw-p p "join") (kw-p p "inner") (kw-p p "left")
               (kw-p p "right") (kw-p p "full") (kw-p p "cross"))
           (when comma-p
             (ereport "0A000" "mixed comma and explicit JOIN syntax is not supported"))
           (setf explicit-p t)
           (push (parse-join-clause p) joins))
          ((match-punct p #\,)
           (when explicit-p
             (ereport "0A000" "mixed comma and explicit JOIN syntax is not supported"))
           (setf comma-p t)
           (multiple-value-bind (source source-alias) (parse-from-relation p)
             (push (list :type :cross :table source :alias source-alias) joins)))
          (t (return))))
      (values table alias (nreverse joins)))))

(defun parse-order-list (p)
  (loop collect (let ((expr (parse-expr p))
                      (desc nil))
                  (cond ((match-kw p "desc") (setf desc t))
                        ((match-kw p "asc") nil))
                  (list :expr expr :desc desc))
        while (match-punct p #\,)))

;;; ---- INSERT / UPDATE / DELETE ----

(defun parse-copy-to (p)
  (let ((query
          (if (match-punct p #\()
              (with-parens (p :opening-consumed t) (parse-select-expression p))
              (let* ((name (pident p))
                     (table (if (match-punct p #\.)
                                (make-sql-relation-name :schema name :name (pident p)) name))
                     (columns (when (match-punct p #\()
                                (with-parens (p :opening-consumed t) (parse-ident-list p)))))
                (when (/= (length columns) (length (remove-duplicates columns :test #'string=)))
                  (ereport "42701" "COPY column specified more than once"))
                (list :type :select :from table
                      :targets (if columns
                                   (mapcar (lambda (column) (list :expr (list :column column)))
                                           columns)
                                   (list (list :star t))))))))
    (unless (match-kw p "to") (ereport "0A000" "only COPY TO STDOUT is supported"))
    (unless (match-kw p "stdout") (ereport "0A000" "only COPY TO STDOUT is supported"))
    (when (match-kw p "with")
      (ereport "0A000" "COPY options are not supported"))
    (list :type :copy-to :query query)))

(defun parse-insert (p)
  (expect-kw p "into")
  (let* ((table (parse-mutation-relation-identifier p))
         (alias (when (match-kw p "as") (pident p)))
         (columns (when (punct-p p #\() (with-parens (p) (parse-ident-list p)))))
    (let ((rows nil) (source nil) (default-values nil))
      (cond ((match-kw p "default")
             (expect-kw p "values") (setf default-values t))
            ((match-kw p "values")
             (setf rows (loop collect (parse-values-row p) while (match-punct p #\,))))
            ((or (select-start-p p) (punct-p p #\())
             (setf source (parse-select-expression p)))
            (t (syntax-error p "expected VALUES or SELECT")))
      (let ((conflict (parse-on-conflict p)))
        (multiple-value-bind (returning options) (parse-returning-clause p)
          (list :type :insert :table table :alias alias :columns columns :rows rows
                :source source :default-values default-values :on-conflict conflict
                :returning returning :returning-options options))))))

(defun parse-values-row (p)
  (with-parens (p)
    (parse-expr-list p)))

(defun parse-ident-list (p)
  (loop collect (pident p) while (match-punct p #\,)))

(defun parse-expr-list (p)
  (loop collect (parse-expr p) while (match-punct p #\,)))

(defun parse-dml-source (p)
  ;; Reuse SELECT source planning while keeping the mutation target separate;
  ;; name analysis later applies the stricter FROM/USING target visibility.
  (multiple-value-bind (from alias joins) (parse-from p)
    (list :type :select :from from :from-alias alias :joins joins :targets '((:star t)))))

(defun parse-update (p)
  (let* ((table (parse-mutation-relation-identifier p))
         (alias (cond ((match-kw p "as") (pident p))
                      ((and (eq (pkind p) :ident) (not (kw-p p "set"))) (pident p)))))
    (expect-kw p "set")
    (let ((sets (loop collect (let ((col (pident p)))
                                (expect-op p "=")
                                (list :column col :expr (parse-expr p)))
                      while (match-punct p #\,)))
          (source nil) (where nil))
      (when (match-kw p "from") (setf source (parse-dml-source p)))
      (when (match-kw p "where")
        (setf where (parse-expr p)))
      (multiple-value-bind (returning options) (parse-returning-clause p)
        (list :type :update :table table :alias alias :sets sets :where where :dml-source source
              :returning returning :returning-options options)))))

(defun parse-delete (p)
  (expect-kw p "from")
  (let* ((table (parse-mutation-relation-identifier p))
         (alias (cond ((match-kw p "as") (pident p))
                      ((and (eq (pkind p) :ident)
                            (or (token-quoted-p (pcur p))
                                (not (member (pval p) '("where" "returning" "using") :test #'equal))))
                       (pident p))))
         (source nil) (where nil))
    (when (match-kw p "using") (setf source (parse-dml-source p)))
    (when (match-kw p "where")
      (setf where (parse-expr p)))
    (multiple-value-bind (returning options) (parse-returning-clause p)
      (list :type :delete :table table :alias alias :where where :dml-source source
            :returning returning :returning-options options))))

(defun parse-truncate (p)
  (match-kw p "table")
  (list :type :truncate
        :tables (loop collect (parse-mutation-relation-identifier p)
                      while (match-punct p #\,))))

;;; ---- CREATE / DROP / ALTER ----

(defun parse-create (p)
  (cond
    ((match-kw p "sequence") (parse-create-sequence p))
    ((match-kw p "publication") (parse-create-publication p))
    ((match-kw p "table") (parse-create-table p))
    ((match-kw p "unique")
     (expect-kw p "index")
     (parse-create-index p t))
    ((match-kw p "index") (parse-create-index p nil))
    (t
     (ereport "0A000" "unsupported CREATE statement"))))

(defun parse-create-sequence (p)
  (let ((if-not-exists
          (when (match-kw p "if")
            (expect-kw p "not")
            (expect-kw p "exists")
            t))
        (name (parse-mutation-relation-identifier p))
        (increment 1)
        (start nil)
        (minimum nil)
        (maximum nil))
    (loop while (and (not (at-eof-p p)) (not (punct-p p #\;)))
          do (cond
               ((match-kw p "increment")
                (match-kw p "by")
                (setf increment (parse-signed-integer-literal p)))
               ((match-kw p "start")
                (match-kw p "with")
                (setf start (parse-signed-integer-literal p)))
               ((match-kw p "minvalue")
                (setf minimum (parse-signed-integer-literal p)))
               ((match-kw p "maxvalue")
                (setf maximum (parse-signed-integer-literal p)))
               (t (ereport "0A000" "unsupported CREATE SEQUENCE option"))))
    (unless (and (/= increment 0)
                 (<= +int8-min+ increment +int8-max+))
      (ereport "22023" "sequence increment must be a nonzero int8"))
    (multiple-value-bind (default-minimum default-maximum)
        (sequence-default-limits increment)
      (setf minimum (or minimum default-minimum)
            maximum (or maximum default-maximum)
            start (or start (if (plusp increment) minimum maximum)))
      (unless (and (sequence-value-range-p minimum)
                   (sequence-value-range-p maximum)
                   (sequence-value-range-p start)
                   (<= minimum start maximum))
        (ereport "22023" "sequence start and bounds must be valid int8 values")))
    (list :type :create-sequence :name name :if-not-exists if-not-exists
          :start start :increment increment :min minimum :max maximum)))

(defun parse-publication-actions (text)
  "Read the publish option's identifier list, as SplitIdentifierString does in C."
  (let* ((parser (make-sql-parser :tokens (tokenize text)))
         (actions nil))
    (unless (at-eof-p parser)
      (loop
        (let* ((name (pident parser))
               (action (cdr (assoc name '(("insert" . :insert) ("update" . :update)
                                          ("delete" . :delete) ("truncate" . :truncate))
                                   :test #'string=))))
          (unless action
            (ereport "42601" "unrecognized value for publication option publish: ~s" name))
          (pushnew action actions))
        (unless (match-punct parser #\,) (return))))
    (unless (at-eof-p parser)
      (syntax-error parser "invalid list syntax in publication option publish"))
    (loop for action in '(:insert :update :delete :truncate)
          when (member action actions) collect action)))

(defun parse-publication-options (p)
  "Parse the supported publication options; refuse unsupported semantics."
  (with-parens (p)
    (let ((actions nil)
          (publish-seen nil))
      (loop
        (let ((name (pident p)))
          (unless (string= name "publish")
            (ereport "0A000" "unsupported publication parameter ~s" name))
          (when publish-seen
            (ereport "42601" "conflicting or redundant publication options"))
          (setf publish-seen t)
          (expect-op p "=")
          (setf actions (parse-publication-actions (parse-string-literal p))))
        (unless (match-punct p #\,) (return)))
      actions)))

(defun parse-create-publication (p)
  (let ((name (pident p))
        (all-tables-p nil)
        (tables '())
        (sequences nil))
    (expect-kw p "for")
    (if (match-kw p "all")
        (cond ((match-kw p "tables") (setf all-tables-p t))
              ((match-kw p "sequences") (setf sequences :all))
              (t (syntax-error p "expected TABLES or SEQUENCES")))
        (progn
          (expect-kw p "table")
          (setf tables (parse-publication-table-list p))))
    (when (match-kw p "sequences")
      (when sequences (syntax-error p "sequences specified more than once"))
      (setf sequences (parse-publication-sequence-list p)))
    (let ((ast (list :type :create-publication :name name :all-tables-p all-tables-p
                     :tables tables :sequences sequences)))
      (when (match-kw p "with")
        (setf (getf ast :publish-actions) (parse-publication-options p)))
      ast)))

(defun parse-publication-table-list (p)
  (loop collect
        (let ((table (parse-mutation-relation-identifier p))
              (filter nil))
          (when (match-kw p "where")
            (setf filter (with-parens (p) (parse-expr p))))
          (list :name table :filter filter))
        while (match-punct p #\,)))

(defun parse-publication-sequence-list (p)
  (if (match-kw p "all")
      :all
      (with-parens (p)
        (loop collect (parse-mutation-relation-identifier p)
              while (match-punct p #\,)))))

(defun parse-create-table (p)
  (let ((if-not-exists
          (when (match-kw p "if")
            (expect-kw p "not")
            (expect-kw p "exists")
            t))
        (name (parse-mutation-relation-identifier p))
        (columns '())
        (constraints '()))
    (expect-punct p #\()
    (loop
      (if (or (kw-p p "primary") (kw-p p "unique") (kw-p p "constraint") (kw-p p "check"))
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

(defun parser-expression-sql (p start)
  "Render a consumed token span without retaining the original statement."
  (format nil "~{~a~^ ~}"
          (loop for token in (subseq (sql-parser-tokens p) start (sql-parser-pos p))
                collect (case (token-kind token)
                          (:ident (if (token-quoted-p token)
                                      (catalog-sql-quote-ident (token-value token))
                                      (token-value token)))
                          (:string (catalog-sql-literal (token-value token)))
                          (:integer (format nil "~d" (token-value token)))
                          (:bit-string (concatenate 'string (subseq (token-value token) 0 1)
                                                    (catalog-sql-literal (subseq (token-value token) 1))))
                          (otherwise (token-value token))))))

(defun parse-stored-expression (p &optional column-default-p)
  (let* ((start (sql-parser-pos p))
         ;; CREATE column DEFAULT uses gram.y's b_expr, so the following NOT
         ;; NULL belongs to the column. Parentheses still admit a full a_expr.
         (expression (if column-default-p
                         (let ((left (parse-concatenation p)))
                           (if (and (eq (pkind p) :op)
                                    (member (pval p) '("=" "<>" "!=" "<" "<=" ">" ">=") :test #'string=))
                               (let ((operator (pval p)))
                                 (pnext p)
                                 (list :binop (if (string= operator "!=") "<>" operator)
                                       left (parse-concatenation p)))
                               left))
                         (parse-expr p))))
    (values expression (parser-expression-sql p start))))

(defun parse-check-body (p &optional name)
  (expect-punct p #\()
  (multiple-value-bind (expression sql) (parse-stored-expression p)
    (expect-punct p #\))
    (list :kind :check :name name :expression expression :sql sql)))

(defun parse-column-def (p)
  (let* ((name (pident p)) (type nil) (typmod -1))
    (multiple-value-setq (type typmod) (parse-type p))
    (let ((notnull nil) (primary nil) (unique nil) (nulls-not-distinct nil)
                        (default-expression nil) (default-sql nil) (checks nil) (constraint-name nil)
                        (primary-name nil) (unique-name nil))
      (loop
        (cond
          ((match-kw p "constraint")
           (when constraint-name (syntax-error p))
           (setf constraint-name (pident p)))
          ((match-kw p "not")
           (expect-kw p "null") (setf notnull t constraint-name nil))
          ((match-kw p "null") (setf constraint-name nil))
          ((match-kw p "primary")
           (expect-kw p "key") (setf primary t notnull t primary-name constraint-name constraint-name nil))
          ((match-kw p "unique")
           (setf unique t nulls-not-distinct (parse-unique-null-treatment p) unique-name constraint-name constraint-name nil))
          ((match-kw p "default")
           (when default-expression (ereport "42601" "multiple default values specified"))
           (multiple-value-setq (default-expression default-sql) (parse-stored-expression p t))
           (setf constraint-name nil))
          ((match-kw p "check")
           (push (parse-check-body p constraint-name) checks)
           (setf constraint-name nil))
          ((match-kw p "references") (ereport "0A000" "foreign keys are not supported"))
          (t (when constraint-name (syntax-error p)) (return))))
      (list :name name :type type :typmod typmod
            :notnull notnull :primary primary :unique unique :primary-name primary-name :unique-name unique-name
            :nulls-not-distinct nulls-not-distinct
            :default-expression default-expression :default-sql default-sql
            :checks (nreverse checks)))))

(defun parse-float-type (p)
  "Resolve SQL FLOAT precision to float4 or float8 as gram.y does."
  (if (match-punct p #\()
      (with-parens (p :opening-consumed t)
        (let ((precision (parse-integer-literal p)))
          (unless (<= 1 precision 53)
            (ereport "22023" "precision for type float must be at least 1 bit and less than 54 bits"))
          (if (<= precision 24) :float4 :float8)))
      :float8))

(defun parse-time-type (p &optional with-zone)
  "Resolve TIME precision and its explicit time zone clause."
  (let ((typmod (if (match-punct p #\()
                    (with-parens (p :opening-consumed t)
                      (let ((precision (parse-integer-literal p)))
                        (when (minusp precision) (ereport "22023" "time precision must not be negative"))
                        (min precision 6)))
                    -1)))
    (cond ((match-kw p "with") (expect-kw p "time") (expect-kw p "zone") (setf with-zone t))
          ((match-kw p "without") (expect-kw p "time") (expect-kw p "zone") (setf with-zone nil)))
    (values (if with-zone :timetz :time) typmod)))

(defun parse-timestamp-type (p &optional with-zone)
  "Resolve TIMESTAMP precision and optional WITH/WITHOUT TIME ZONE."
  (let ((typmod (if (match-punct p #\()
                    (with-parens (p :opening-consumed t)
                      (min 6 (parse-integer-literal p)))
                    -1)))
    (cond ((match-kw p "with") (expect-kw p "time") (expect-kw p "zone") (setf with-zone t))
          ((match-kw p "without") (expect-kw p "time") (expect-kw p "zone") (setf with-zone nil)))
    (values (if with-zone :timestamptz :timestamp) typmod)))

(defun parse-interval-typmod (p)
  "Parse SQL interval field ranges and fractional second precision."
  (labels ((precision ()
             (with-parens (p)
               (let ((value (parse-signed-integer-literal p)))
                 (when (minusp value) (ereport "22023" "interval precision must not be negative"))
                 (min value 6))))
           (field ()
             (cond ((match-kw p "year") :year) ((match-kw p "month") :month)
                   ((match-kw p "day") :day) ((match-kw p "hour") :hour)
                   ((match-kw p "minute") :minute) ((match-kw p "second") :second)))
           (mask (field)
             (ecase field (:year 4) (:month 2) (:day 8) (:hour 1024) (:minute 2048) (:second 4096))))
    (when (punct-p p #\()
      (return-from parse-interval-typmod (logior (ash #x7fff 16) (precision))))
    (let ((start (field)))
      (unless start (return-from parse-interval-typmod -1))
      (let* ((to-p (match-kw p "to"))
             (end (if to-p
                      (or (field) (syntax-error p "expected interval field"))
                      start))
             (valid (or (not to-p)
                        (member (list start end)
                                '((:year :month) (:day :hour) (:day :minute) (:day :second)
                                  (:hour :minute) (:hour :second) (:minute :second))
                                :test #'equal)))
             (range (cond ((eq start end) (mask start))
                          ((equal (list start end) '(:year :month)) 6)
                          ((eq start :day)
                           (logior 8 1024 (if (member end '(:minute :second)) 2048 0)
                                   (if (eq end :second) 4096 0)))
                          ((eq start :hour) (logior 1024 2048 (if (eq end :second) 4096 0)))
                          ((eq start :minute) (logior 2048 4096))
                          (t 0)))
             (fraction (if (and (eq end :second) (punct-p p #\()) (precision) #xffff)))
        (unless valid (syntax-error p "invalid interval field range"))
        (logior (ash range 16) fraction)))))

(defun parse-type (p)
  (when (token-quoted-p (pcur p))
    (let ((name (pident p)))
      (return-from parse-type
        (cond ((string= name "int2") (values :int2 -1))
              ((string= name "interval") (values :interval (parse-interval-generic-typmod p)))
              ((string= name "date") (values :date -1))
              ((string= name "int4") (values :int4 -1))
              ((string= name "int8") (values :int8 -1))
              ((string= name "float4") (values :float4 -1))
              ((string= name "float8") (values :float8 -1))
              ((string= name "numeric") (values :numeric (parse-numeric-typmod p)))
              ((string= name "varchar") (values :varchar (parse-varchar-typmod p)))
              ((string= name "text") (values :varchar 130))
              (t (ereport "0A000" "unsupported storage type \"~a\"" name))))))
  (cond
    ((match-kw p "date") (values :date -1))
    ((match-kw p "interval") (values :interval (parse-interval-typmod p)))
    ((match-kw p "time") (parse-time-type p))
    ((match-kw p "timetz") (parse-time-type p t))
    ((match-kw p "timestamp") (parse-timestamp-type p))
    ((match-kw p "timestamptz") (parse-timestamp-type p t))
    ((or (match-kw p "smallint") (match-kw p "int2")) (values :int2 -1))
    ((match-kw p "int") (values :int4 -1))
    ((match-kw p "integer") (values :int4 -1))
    ((match-kw p "int4") (values :int4 -1))
    ((match-kw p "bigint") (values :int8 -1))
    ((match-kw p "int8") (values :int8 -1))
    ((match-kw p "float") (values (parse-float-type p) -1))
    ((or (match-kw p "real") (match-kw p "float4")) (values :float4 -1))
    ((or (match-kw p "float8") (and (match-kw p "double") (progn (expect-kw p "precision") t)))
     (values :float8 -1))
    ((or (match-kw p "numeric") (match-kw p "decimal"))
     (values :numeric (parse-numeric-typmod p)))
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
      ((match-kw p "check") (parse-check-body p cname))
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
        (name (parse-mutation-relation-identifier p)))
    (expect-kw p "on")
    (let ((table (parse-mutation-relation-identifier p)))
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
    ((match-kw p "publication")
     (let ((if-exists (when (match-kw p "if")
                        (expect-kw p "exists")
                        t)))
       (list :type :drop-publication
             :names (loop collect (pident p)
                          while (match-punct p #\,))
             :if-exists if-exists)))
    ((match-kw p "sequence")
     (let ((if-exists (when (match-kw p "if")
                        (expect-kw p "exists")
                        t)))
       (list :type :drop-sequence :names
             (loop collect (parse-mutation-relation-identifier p)
                   while (match-punct p #\,))
             :if-exists if-exists)))
    ((match-kw p "table")
     (let ((if-exists (when (match-kw p "if")
                        (expect-kw p "exists")
                        t)))
       (let ((names (loop collect (parse-mutation-relation-identifier p)
                          while (match-punct p #\,))))
         (match-kw p "cascade")
         (match-kw p "restrict")
         (list :type :drop-table :names names :if-exists if-exists))))
    ((match-kw p "index")
     (let ((if-exists (when (match-kw p "if")
                        (expect-kw p "exists")
                        t)))
       (let ((names (loop collect (parse-mutation-relation-identifier p)
                          while (match-punct p #\,))))
         (match-kw p "cascade")
         (match-kw p "restrict")
         (list :type :drop-index :names names :if-exists if-exists))))
    (t
     (ereport "0A000" "unsupported DROP statement"))))

(defun parse-alter (p)
  (cond
    ((match-kw p "publication")
     (parse-alter-publication p))
    ((match-kw p "sequence")
     (let ((name (parse-mutation-relation-identifier p)))
       (cond
         ((match-kw p "increment")
          (match-kw p "by")
          (list :type :alter-sequence :name name
                :increment (parse-signed-integer-literal p)))
         ((match-kw p "restart")
          (match-kw p "with")
          (list :type :alter-sequence :name name
                :restart (parse-signed-integer-literal p)))
         (t (ereport "0A000" "unsupported ALTER SEQUENCE option")))))
    ((match-kw p "index")
     (let ((name (parse-mutation-relation-identifier p)))
       (expect-kw p "rename")
       (expect-kw p "to")
       (list :type :alter-index :name name :new-name (pident p))))
    ((match-kw p "table")
     (parse-alter-table p))
    (t
     (syntax-error p "expected TABLE or INDEX after ALTER"))))

(defun parse-alter-publication (p)
  (let ((name (pident p)))
    (cond
      ((match-kw p "add")
       (cond
         ((match-kw p "table")
          (list :type :alter-publication :name name :action :add-table
                :tables (parse-publication-table-list p)))
         ((match-kw p "sequence")
          (list :type :alter-publication :name name :action :add-sequence
                :sequences (loop collect (parse-mutation-relation-identifier p)
                                 while (match-punct p #\,))))
         (t (ereport "0A000" "ALTER PUBLICATION ADD supports TABLE or SEQUENCE"))))
      ((match-kw p "drop")
       (cond
         ((match-kw p "table")
          (list :type :alter-publication :name name :action :drop-table
                :tables (loop collect (list :name (parse-mutation-relation-identifier p))
                              while (match-punct p #\,))))
         ((match-kw p "sequence")
          (list :type :alter-publication :name name :action :drop-sequence
                :sequences (loop collect (parse-mutation-relation-identifier p)
                                 while (match-punct p #\,))))
         (t (ereport "0A000" "ALTER PUBLICATION DROP supports TABLE or SEQUENCE"))))
      ((match-kw p "set")
       (cond
         ((match-kw p "table")
          (list :type :alter-publication :name name :action :set-table
                :tables (parse-publication-table-list p)))
         ((match-kw p "sequences")
          (list :type :alter-publication :name name :action :set-sequences
                :sequences (parse-publication-sequence-list p)))
         ((punct-p p #\()
          (list :type :alter-publication :name name :action :set-options
                :publish-actions (parse-publication-options p)))
         (t (ereport "0A000" "ALTER PUBLICATION SET supports TABLE, SEQUENCES or options"))))
      (t (ereport "0A000" "unsupported ALTER PUBLICATION action")))))

(defun parse-alter-table (p)
  (let ((if-exists (when (match-kw p "if")
                     (expect-kw p "exists")
                     t))
        (table (parse-mutation-relation-identifier p)))
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
    ((match-kw p "alter")
     (match-kw p "column")
     (let ((column (pident p)))
       (cond ((match-kw p "set")
              (expect-kw p "default")
              (multiple-value-bind (expression sql) (parse-stored-expression p)
                (list :action :set-default :column column :expression expression :sql sql)))
             ((match-kw p "drop")
              (expect-kw p "default") (list :action :drop-default :column column))
             (t (syntax-error p)))))
    ((match-kw p "validate")
     (expect-kw p "constraint")
     (list :action :validate-constraint :constraint-name (pident p)))
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
         ((match-kw p "check")
          (let ((check (parse-check-body p cname)))
            (when (match-kw p "not") (expect-kw p "valid") (setf (getf check :not-valid) t))
            (list :action :add-check :check check)))
         ((match-kw p "primary")
          (expect-kw p "key")
          (if (match-kw p "using")
              (progn
                (expect-kw p "index")
                (list :action :add-primary-key
                      :using-index (parse-mutation-relation-identifier p)
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
                        :using-index (parse-mutation-relation-identifier p)
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
       ((match-kw p "committed") :read-committed)
       ((match-kw p "uncommitted")
        (ereport "0A000" "read uncommitted is not supported"))
       (t (syntax-error p))))
    ((match-kw p "repeatable")
     (expect-kw p "read")
     :repeatable-read)
    ((match-kw p "serializable")
     (ereport "0A000" "serializable is not supported"))
    (t (syntax-error p))))

(defun transaction-characteristic-start-p (p)
  (or (kw-p p "isolation") (kw-p p "read")))

(defun parse-transaction-characteristics (p)
  "Parse the supported per-transaction isolation and access-mode options."
  (let ((isolation-level nil)
        (isolation-seen nil)
        (read-only nil)
        (read-mode-seen nil))
    (loop while (transaction-characteristic-start-p p)
          do (cond
               ((kw-p p "isolation")
                (when isolation-seen
                  (ereport "42601" "transaction isolation level specified more than once"))
                (setf isolation-level (parse-isolation-level p)
                      isolation-seen t))
               ((match-kw p "read")
                (when read-mode-seen
                  (ereport "42601" "transaction access mode specified more than once"))
                (setf read-only
                      (cond ((match-kw p "only") t)
                            ((match-kw p "write") nil)
                            (t (syntax-error p "expected ONLY or WRITE")))
                      read-mode-seen t))
               (t (return)))
             (when (match-punct p #\,)
               (unless (transaction-characteristic-start-p p)
                 (syntax-error p "expected transaction characteristic after comma"))))
    (append (when isolation-seen (list :isolation-level isolation-level))
            (when read-mode-seen (list :read-only read-only)))))

(defun parse-set-value (p)
  (cond
    ((or (op-p p "-") (op-p p "+"))
     (let ((sign (pval p)))
       (pnext p)
       (unless (eq (pkind p) :integer) (syntax-error p "expected setting value"))
       (concatenate 'string sign (princ-to-string (token-value (pnext p))))))
    ((eq (pkind p) :numeric) (ereport "0A000" "decimal numeric literals are not supported"))
    ((eq (pkind p) :integer)
     (princ-to-string (token-value (pnext p))))
    ((eq (pkind p) :string)
     (token-value (pnext p)))
    ((eq (pkind p) :ident)
     (if (kw-p p "default")
         (progn (pnext p) :default)
         (pident p)))
    (t (syntax-error p "expected setting value"))))

(defun parse-search-path-setting (p)
  "Keep SET's list elements distinct from commas within a quoted SQL value."
  (let ((values (loop collect (parse-set-value p) while (match-punct p #\,))))
    (if (equal values '(:default))
        :default
        (progn
          (when (member :default values)
            (syntax-error p "DEFAULT cannot be combined with other search_path values"))
          (format nil "~{~a~^, ~}"
                  (mapcar (lambda (name)
                            (if (member name '("public" "pg_catalog") :test #'string=)
                                name
                                (catalog-sql-quote-ident name)))
                          values))))))

(defun parse-set (p)
  (let ((session-p (match-kw p "session")))
    (when (match-kw p "local")
      (when session-p (syntax-error p "SESSION and LOCAL are alternatives"))
      (let ((name (pident p)))
        (unless (string-equal name "application_name")
          (ereport "0A000" "SET LOCAL is only supported for application_name"))
        (when (eq (pkind p) :op) (pnext p))
        (match-kw p "to")
        (return-from parse-set
          (list :type :set :name name :local-p t :value (parse-set-value p)))))
    (cond
      ((match-kw p "transaction")
       (when session-p
         (ereport "0A000" "session transaction characteristics are not supported"))
       (if (match-kw p "snapshot")
           (list :type :set-transaction-snapshot
                 :token (parse-set-value p))
           (let ((characteristics (parse-transaction-characteristics p)))
             (unless characteristics
               (syntax-error p "expected transaction characteristic"))
             (list :type :set :transaction-options-p t
                   :transaction-options characteristics))))
      ((match-kw p "time")
       (expect-kw p "zone")
       (list :type :set :name "timezone" :value (parse-set-value p)))
      ((match-kw p "characteristics")
       (expect-kw p "as")
       (expect-kw p "transaction")
       (let ((isolation-level (parse-isolation-level p)))
         (unless (eq isolation-level :read-committed)
           (ereport "0A000" "session transaction characteristics are not supported")))
       (list :type :set :session-characteristics-p t
             :name "transaction_isolation" :value "read committed"))
      (t
       (let ((name (pident p)))
         (when (eq (pkind p) :op)
           (pnext p))
         (match-kw p "to")
         (list :type :set :name name
               :value (if (string-equal name "search_path")
                          (parse-search-path-setting p)
                          (parse-set-value p))))))))

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

(defun select-start-p (p)
  ;; Look through parentheses only when they form a complete query operand.
  "Recognize a complete subquery operand, preserving scalar expression continuations."
  (when (or (kw-p p "select") (kw-p p "with")) (return-from select-start-p t))
  (let ((lookahead (copy-sql-parser p)))
    (loop while (match-punct lookahead #\())
    (unless (or (kw-p lookahead "select") (kw-p lookahead "with")) (return-from select-start-p nil)))
  (let ((lookahead (copy-sql-parser p)))
    (handler-case
        (progn (parse-select-expression lookahead) (punct-p lookahead #\)))
      (pg-error (condition)
        (unless (string= (pg-error-sqlstate condition) "42601")
          (error condition))))))

(defun parse-quantified-operand (p kind operator left)
  (with-parens (p)
    (if (select-start-p p)
        (list :sublink kind (parse-select-expression p) left operator)
        (list (if (eq kind :any) :any :all) operator left (parse-expr p)))))

(defun parse-comparison (p)
  (let ((left (parse-concatenation p)))
    (cond
      ((and (eq (pkind p) :op)
            (member (pval p) '("=" "<>" "!=" "<" "<=" ">" ">=") :test #'string=))
       (let ((op (if (string= (pval p) "!=") "<>" (pval p))))
         (pnext p)
         (cond
           ((or (match-kw p "any") (match-kw p "some"))
            (parse-quantified-operand p :any op left))
           ((match-kw p "all") (parse-quantified-operand p :all op left))
           (t (list :binop op left (parse-concatenation p))))))
      ((or (kw-p p "in") (kw-p p "not"))
       (let ((negated (match-kw p "not")))
         (expect-kw p "in")
         (with-parens (p)
           (if (select-start-p p)
               (let ((query (list :sublink :any (parse-select-expression p) left "=")))
                 (if negated (list :not query) query))
               (list :in left (parse-expr-list p) negated)))))
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

(defun parse-concatenation (p)
  (parse-binary-level p '("||") #'parse-additive))

(defun parse-additive (p)
  (parse-binary-level p '("+" "-") #'parse-multiplicative))

(defun parse-multiplicative (p)
  (parse-binary-level p '("*" "/" "%") #'parse-power))

(defun parse-power (p)
  "Parse left-associative power between unary and multiplicative precedence."
  (let ((left (parse-unary p)))
    (loop while (and (eq (pkind p) :op)
                     (plusp (length (pval p)))
                     (char= (char (pval p) 0) #\^))
          do (let ((operator (pval p)))
               (pnext p)
               (setf left (list :binop operator left (parse-unary p)))))
    left))

(defun parse-expression-varchar-typmod (p)
  (if (match-punct p #\()
      (with-parens (p :opening-consumed t)
        (let ((length (parse-integer-literal p)))
          (unless (<= 1 length 10485760)
            (ereport "22023" "length for type varchar is out of range"))
          (+ length 4)))
      -1))

(defun type-modifier-input-string (expression)
  "Match typenameTypeMod's constant/identifier validation before integer input."
  (cond
    ((eq (first expression) :numeric-literal) (second expression))
    ((and (eq (first expression) :const) (integerp (second expression)))
     (format nil "~d" (second expression)))
    ((and (eq (first expression) :const) (stringp (second expression)))
     (second expression))
    ((and (eq (first expression) :column) (= (length expression) 2))
     (second expression))
    (t (ereport "42601" "type modifiers must be simple constants or identifiers"))))

(defun parse-bit-typmod (p default)
  (if (match-punct p #\()
      (let* ((strings (mapcar #'type-modifier-input-string
                              (let ((*parse-type-modifiers-p* t))
                                (with-parens (p :opening-consumed t) (parse-expr-list p)))))
             (lengths (mapcar (lambda (text) (sql-cast-integer-input text :int4)) strings)))
        (unless (= (length lengths) 1) (ereport "22023" "invalid type modifier"))
        (sql-bit-validate-typmod (first lengths)))
      default))

(defun parse-numeric-typmod (p)
  "Parse and pack numeric precision and scale as in numerictypmodin."
  (if (match-punct p #\()
      (let* ((strings (mapcar #'type-modifier-input-string
                              (let ((*parse-type-modifiers-p* t))
                                (with-parens (p :opening-consumed t) (parse-expr-list p)))))
             (numbers (mapcar (lambda (text) (sql-cast-integer-input text :int4)) strings)))
        (unless (<= 1 (length numbers) 2)
          (ereport "22023" "invalid NUMERIC type modifier"))
        (let ((precision (first numbers)) (scale (or (second numbers) 0)))
          (unless (<= 1 precision 1000)
            (ereport "22023" "NUMERIC precision ~d must be between 1 and 1000" precision))
          (unless (<= -1000 scale 1000)
            (ereport "22023" "NUMERIC scale ~d must be between -1000 and 1000" scale))
          (+ 4 (logior (ash precision 16) (logand scale #x7ff)))))
      -1))

(defun parse-interval-generic-typmod (p)
  "Parse the raw range and precision arguments of a qualified interval type."
  (unless (match-punct p #\() (return-from parse-interval-generic-typmod -1))
  (let* ((strings (mapcar #'type-modifier-input-string
                          (let ((*parse-type-modifiers-p* t))
                            (with-parens (p :opening-consumed t) (parse-expr-list p)))))
         (numbers (mapcar (lambda (text) (sql-cast-integer-input text :int4)) strings))
         (range (first numbers)) (precision (second numbers)))
    (unless (and (<= 1 (length numbers) 2)
                 (member range '(4 2 8 1024 2048 4096 6 1032 3080 7176 3072 7168 6144 32767)))
      (ereport "22023" "invalid INTERVAL type modifier"))
    (when (and precision (minusp precision)) (ereport "22023" "interval precision must not be negative"))
    (if (and (= range 32767) (null precision)) -1
        (logior (ash range 16) (if precision (min precision 6) #xffff)))))

(defun parse-scalar-cast-type (p)
  "Resolve supported expression types independently of table storage aliases."
  (let* ((quoted (token-quoted-p (pcur p)))
         (name (pident p))
         (qualified (match-punct p #\.)))
    (when qualified
      (unless (string= name "pg_catalog")
        (ereport "42704" "type \"~a.~a\" does not exist" name (pval p)))
      (setf quoted (token-quoted-p (pcur p)) name (pident p)))
    (cond
      ((or (string= name "numeric")
           (and (not quoted) (not qualified) (string= name "decimal")))
       (values :numeric (parse-numeric-typmod p)))
      ((and (not quoted) (not qualified) (string= name "float"))
       (values (parse-float-type p) -1))
      ((or (string= name "float4")
           (and (not quoted) (not qualified) (string= name "real"))) (values :float4 -1))
      ((or (string= name "float8")
           (and (not quoted) (not qualified) (string= name "double") (match-kw p "precision")))
       (values :float8 -1))
      ((string= name "date") (values :date -1))
      ((string= name "interval") (values :interval (if (or quoted qualified) (parse-interval-generic-typmod p) (parse-interval-typmod p))))
      ((string= name "time") (parse-time-type p))
      ((string= name "timetz") (parse-time-type p t))
      ((string= name "timestamp") (parse-timestamp-type p))
      ((string= name "timestamptz") (parse-timestamp-type p t))
      ((string= name "text") (values :text -1))
      ((string= name "name") (values :name -1))
      ((string= name "oid") (values :oid -1))
      ((string= name "pg_lsn") (values :pg-lsn -1))
      ((string= name "bit")
       (when (and (not quoted) (not qualified) (match-kw p "varying"))
         (ereport "0A000" "bit varying casts are not implemented"))
       (values :bit (parse-bit-typmod p (if (or quoted qualified) -1 1))))
      ((or (string= name "int2")
           (and (not quoted) (not qualified) (string= name "smallint")))
       (values :int2 -1))
      ((string= name "int2vector") (values :int2vector -1))
      ((and (string= name "char") (or quoted qualified)) (values :char -1))
      ((or (string= name "int4")
           (and (not quoted) (not qualified) (member name '("int" "integer") :test #'string=)))
       (values :int4 -1))
      ((or (string= name "int8")
           (and (not quoted) (not qualified) (string= name "bigint")))
       (values :int8 -1))
      ((or (string= name "bool")
           (and (not quoted) (not qualified) (string= name "boolean")))
       (values :bool -1))
      ((string= name "varchar") (values :varchar (parse-expression-varchar-typmod p)))
      ((and (not quoted) (not qualified) (string= name "character")
            (match-kw p "varying"))
       (values :varchar (parse-expression-varchar-typmod p)))
      (t (ereport "0A000" "unsupported expression type \"~a\"" name)))))

(defun parse-cast-type (p)
  (multiple-value-bind (type typmod) (parse-scalar-cast-type p)
    (when (match-punct p #\[)
      (expect-punct p #\])
      (unless (eq type :text)
        (ereport "0A000" "only text array casts are supported"))
      (setf type :text-array typmod -1))
    (values type typmod)))

(defun parse-postfix-expression (p)
  (let ((expression (parse-primary p)))
    (loop
      (cond
        ((match-op p "::")
         (multiple-value-bind (type typmod) (parse-cast-type p)
           (setf expression (list :cast expression type typmod))))
        ((match-punct p #\.)
         (setf expression
               (if (match-op p "*") (list :record-expand expression)
                   (list :record-field expression (pident p)))))
        (t (return expression))))))

(defun parse-unary (p)
  (cond
    ((match-op p "-")
     (let ((argument (parse-unary p)))
       ;; gram.y:doNegate folds raw constants before selecting their type.
       (cond
         ((and (eq (first argument) :const) (integerp (second argument)))
          (list :const (- (second argument))))
         ((eq (first argument) :numeric-literal)
          (let ((text (second argument)))
            (list :numeric-literal
                  (if (char= (char text 0) #\-) (subseq text 1)
                      (concatenate 'string "-" text)))))
         (t (list :unary "-" argument)))))
    ((match-op p "+")
     (let ((argument (parse-unary p)))
       (if *parse-type-modifiers-p* (list :unary "+" argument) argument)))
    (t (parse-postfix-expression p))))

(defun parse-call-argument (parser)
  "Parse a positional argument or a named argument using => or :=."
  (let ((next (nth (1+ (sql-parser-pos parser)) (sql-parser-tokens parser))))
    (if (and (eq (pkind parser) :ident) next
             (eq (token-kind next) :op)
             (member (token-value next) '("=>" ":=") :test #'string=))
        (let ((name (pident parser)))
          (pnext parser)
          (list :named-arg name (parse-expr parser)))
        (parse-expr parser))))

(defun parse-call-arguments (p)
  "Read arguments after the opening parenthesis; VARIADIC may occur only last."
  (if (match-punct p #\))
      (values nil nil)
      (with-parens (p :opening-consumed t)
        (loop with arguments = nil
              for variadic = (and (not (token-quoted-p (pcur p))) (match-kw p "variadic"))
              do (push (parse-call-argument p) arguments)
              when variadic do (return (values (nreverse arguments) t))
                unless (match-punct p #\,) do (return (values (nreverse arguments) nil))))))

(defun parse-function-call (p name &optional schema)
  "Parse an ordinary call or an OVER expression."
  (when (kw-p p "distinct")
    (ereport "0A000" "DISTINCT function arguments are not supported"))
  (let ((call
          (if (and (member name '("count" "sum") :test #'string=)
                   (or (null schema) (string= schema "pg_catalog"))
                   (match-op p "*"))
              (progn
                (expect-punct p #\))
                (if (string= name "count") (list :count-star)
                    (append (list :func name nil) (when schema (list :schema schema)))))
              (multiple-value-bind (arguments variadic) (parse-call-arguments p)
                (append (list :func name arguments)
                        (when schema (list :schema schema))
                        (when variadic (list :variadic t)))))))
    (when (and (kw-p p "filter")
               (let ((probe (copy-sql-parser p)))
                 (pnext probe)
                 (punct-p probe #\()))
      (ereport "0A000" "aggregate FILTER clauses are not supported"))
    (if (match-kw p "over")
        (list :window call (if (punct-p p #\() (parse-window-definition p) (pident p)))
        call)))

(defun parse-case-expression (p)
  "Read searched CASE; a CASE value expression remains outside this SQL subset."
  (unless (kw-p p "when")
    (ereport "0A000" "only searched CASE is supported"))
  (let ((branches nil))
    (loop while (match-kw p "when")
          do (let ((condition (parse-expr p)))
               (expect-kw p "then")
               (push (list condition (parse-expr p)) branches)))
    (let ((otherwise (if (match-kw p "else") (parse-expr p) (list :const +null+))))
      (expect-kw p "end")
      (list :case (nreverse branches) otherwise))))

(defun parse-array-expression (p)
  (expect-punct p #\[)
  (let ((items nil))
    (unless (punct-p p #\])
      (push (parse-expr p) items)
      (loop while (match-punct p #\,) do (push (parse-expr p) items)))
    (expect-punct p #\])
    (list :array-literal (nreverse items))))

(defun parse-primary (p)
  (cond
    ((eq (pkind p) :numeric)
     (list :numeric-literal (token-value (pnext p))))
    ((eq (pkind p) :bit-string) (list :bit-literal (token-value (pnext p))))
    ((eq (pkind p) :integer)
     (list :const (token-value (pnext p))))
    ((eq (pkind p) :string)
     (list :const (token-value (pnext p))))
    ((match-punct p #\()
     (with-parens (p :opening-consumed t)
       (if (or (kw-p p "select") (kw-p p "with"))
           (list :sublink :scalar (parse-select-expression p) nil nil)
           (let ((expression (parse-expr p)))
             (if (and (eq (first expression) :sublink)
                      (eq (second expression) :scalar) (kw-p p "union"))
                 (list :sublink :scalar (parse-select-tail p (third expression)) nil nil)
                 expression)))))
    ((and (not (token-quoted-p (pcur p))) (match-kw p "cast"))
     (with-parens (p)
       (let ((argument (parse-expr p)))
         (expect-kw p "as")
         (multiple-value-bind (type typmod) (parse-cast-type p)
           (list :cast argument type typmod)))))
    ((and (not (token-quoted-p (pcur p))) (match-kw p "coalesce"))
     (with-parens (p)
       (list :coalesce (parse-expr-list p))))
    ((match-kw p "case") (parse-case-expression p))
    ((match-kw p "exists")
     (list :sublink :exists (with-parens (p) (parse-select-expression p)) nil nil))
    ((match-kw p "array")
     (if (punct-p p #\()
         (list :sublink :array (with-parens (p) (parse-select-expression p)) nil nil)
         (parse-array-expression p)))
    ((match-kw p "null") (list :const +null+))
    ((match-kw p "default") (list :default))
    ((match-kw p "true") (list :bool t))
    ((match-kw p "false") (list :bool nil))
    ((and (kw-p p "interval")
          (let ((next (nth (1+ (sql-parser-pos p)) (sql-parser-tokens p))))
            (and next (eq (token-kind next) :punct) (string= (token-value next) "("))))
     (pnext p)
     (let ((typmod (parse-interval-typmod p)))
       (unless (eq (pkind p) :string) (syntax-error p "expected interval literal"))
       (list :cast (list :const (token-value (pnext p))) :interval typmod)))
    ((and (not (token-quoted-p (pcur p)))
          (or (kw-p p "date") (kw-p p "time") (kw-p p "timestamp") (kw-p p "timestamptz") (kw-p p "interval"))
          (eq (token-kind (nth (1+ (sql-parser-pos p)) (sql-parser-tokens p))) :string))
     (let ((type (cond ((kw-p p "date") :date) ((kw-p p "time") :time)
                       ((kw-p p "timestamp") :timestamp) ((kw-p p "interval") :interval) (t :timestamptz))))
       (pnext p)
       (let ((value (token-value (pnext p))))
         (list :cast (list :const value) type (if (eq type :interval) (parse-interval-typmod p) -1)))))
    ((eq (pkind p) :ident)
     (let* ((quoted (token-quoted-p (pcur p)))
            (name (pident p)))
       (cond
         ((match-punct p #\.)
          (let ((col (pident p)))
            (if (match-punct p #\()
                (parse-function-call p col name)
                (list :column name col))))
         ((match-punct p #\() (parse-function-call p name))
         ((and (not quoted) (member name '("current_user" "session_user" "user"
                                           "current_database" "current_schema" "current_catalog")
                                    :test #'string=))
          (list :func name '()))
         (t (list :column name)))))
    (t (syntax-error p))))
