;;;; analyze.lisp -- name resolution and expression evaluation.

(in-package #:pg)

(defstruct select-relation
  name
  alias
  table
  (offset 0)
  (width 0))

(defstruct select-column
  name
  expr
  type
  (typmod -1)
  (table-oid 0)
  (attnum 0))

(defstruct select-scope
  relations
  columns
  (width 0))

(defun select-relation-qualifier (relation)
  (or (select-relation-alias relation)
      (select-relation-name relation)))

(defun resolve-select-column (scope qualifier name)
  (if qualifier
      (let* ((relation
               (find qualifier (select-scope-relations scope)
                     :key #'select-relation-qualifier :test #'string=))
             (column
               (and relation
                    (find-column (select-relation-table relation) name))))
        (unless relation
          (ereport "42P01" "missing FROM-clause entry for table \"~a\""
                   qualifier))
        (unless column
          (ereport "42703" "column \"~a\" does not exist" name))
        (list :col (+ (select-relation-offset relation)
                      (pg-column-attnum column))
              :type (pg-column-type column)
              :typmod (pg-column-typmod column)
              :notnull (pg-column-notnull column)
              :table-oid (pg-class-oid (select-relation-table relation))
              :attnum (pg-column-attnum column)))
      (let ((matches (remove-if-not
                      (lambda (column)
                        (string= name (select-column-name column)))
                      (select-scope-columns scope))))
        (cond
          ((null matches)
           (ereport "42703" "column \"~a\" does not exist" name))
          ((rest matches)
           (ereport "42702" "column reference \"~a\" is ambiguous" name))
          (t
           (select-column-expr (first matches)))))))

(defmacro sql-strict-let (bindings &body body)
  "Evaluate all BINDINGS once, returning SQL NULL if any value is NULL."
  `(let* ,bindings
     (if (or ,@(mapcar (lambda (binding)
                         `(eq ,(first binding) +null+))
                       bindings))
         +null+
         (progn ,@body))))

(defun type-oid (type)
  (case type
    (:int4 +int4oid+)
    (:int8 +int8oid+)
    (:bool +booloid+)
    (:varchar +varcharoid+)
    (t +textoid+)))

(defun expr-type (expr)
  (let ((tail (member :type expr)))
    (and tail (second tail))))
(defun expr-typmod (expr)
  (let ((tail (member :typmod expr)))
    (and tail (second tail))))

(defun typed-arg-name (expr)
  "PostgreSQL-style name for an argument's type, for \"function ... does not
exist\" messages."
  (case (expr-type expr)
    (:int4 "integer")
    (:int8 "bigint")
    (:varchar "character varying")
    (:bool "boolean")
    (t "text")))

(defun call-signature (args)
  (format nil "~{~a~^, ~}" (mapcar #'typed-arg-name args)))

(defun admin-arg-coercible-p (expr target-type)
  "Whether EXPR can resolve to an admin function's declared integer type."
  (or (and (eq (first expr) :const)
           (or (eq (second expr) +null+)
               (stringp (second expr))))
      (case target-type
        (:int4 (eq (expr-type expr) :int4))
        (:int8 (member (expr-type expr) '(:int4 :int8))))))

(defun analyze-expr (expr table)
  "Resolve column references and annotate expression types."
  (case (first expr)
    (:const
     (let ((v (second expr)))
       (cond
         ((eq v +null+) (list :const v :type :text :typmod -1))
         ((integerp v)
          (list :const v
                :type (if (<= +int4-min+ v +int4-max+) :int4 :int8)
                :typmod -1))
         ((stringp v) (list :const v :type :text :typmod -1))
         (t (list :const v :type :text :typmod -1)))))
    (:bool
     (list :bool (second expr) :type :bool :typmod -1))
    (:column
     (let ((name (if (third expr) (third expr) (second expr)))
           (qualifier (and (third expr) (second expr))))
       (cond
         ((typep table 'select-scope)
          (resolve-select-column table qualifier name))
         (t
          (unless table
            (ereport "42703" "column \"~a\" does not exist" name))
          (let ((col (find-column table name)))
            (unless col
              (ereport "42703" "column \"~a\" does not exist" name))
            (list :col (pg-column-attnum col)
                  :type (pg-column-type col)
                  :typmod (pg-column-typmod col)
                  :notnull (pg-column-notnull col)
                  :table-oid (pg-class-oid table)
                  :attnum (pg-column-attnum col)))))))
    (:binop
     (let* ((op (second expr))
            (l (analyze-expr (third expr) table))
            (r (analyze-expr (fourth expr) table)))
       (if (member op '("=" "<>" "<" "<=" ">" ">=") :test #'string=)
           (list :binop op l r :type :bool :typmod -1)
           (list :binop op l r
                 :type (if (or (eq (expr-type l) :int8)
                               (eq (expr-type r) :int8))
                           :int8 :int4)
                 :typmod -1))))
    (:unary
     (let ((e (analyze-expr (third expr) table)))
       (list :unary (second expr) e
             :type (expr-type e) :typmod (expr-typmod e))))
    (:and
     (list :and (analyze-expr (second expr) table)
           (analyze-expr (third expr) table)
           :type :bool :typmod -1))
    (:or
     (list :or (analyze-expr (second expr) table)
           (analyze-expr (third expr) table)
           :type :bool :typmod -1))
    (:not
     (list :not (analyze-expr (second expr) table)
           :type :bool :typmod -1))
    (:is-null
     (list :is-null (analyze-expr (second expr) table) (third expr)
           :type :bool :typmod -1))
    (:is-bool
     (list :is-bool (analyze-expr (second expr) table) (third expr)
           (fourth expr) :type :bool :typmod -1))
    (:func
     (let* ((name (second expr))
            (args (mapcar (lambda (a) (analyze-expr a table)) (third expr))))
       (cond
         ((member name '("version" "pg_lisp_runtime"
                         "current_database" "current_schema"
                         "current_catalog" "current_user" "session_user" "user")
                  :test #'string=)
          (when args
            (ereport "42883" "function ~a(~a) does not exist"
                     name (call-signature args)))
          (list :func name args :type :text :typmod -1))
         ((member name '("length" "char_length" "character_length")
                  :test #'string=)
          (unless (= (length args) 1)
            (ereport "42883" "function ~a(~a) does not exist"
                     name (call-signature args)))
          (list :func name args :type :int4 :typmod -1))
         ((string= name "pg_backend_pid")
          (when args
            (ereport "42883" "function ~a(~a) does not exist"
                     name (call-signature args)))
          (list :func name args :type :int4 :typmod -1))
         ((string= name "pg_cancel_backend")
          (unless (and (= (length args) 1)
                       (admin-arg-coercible-p (first args) :int4))
            (ereport "42883" "function ~a(~a) does not exist"
                     name (call-signature args)))
          (list :func name args :type :bool :typmod -1))
         ((string= name "pg_terminate_backend")
          (unless (and (<= 1 (length args) 2)
                       (admin-arg-coercible-p (first args) :int4)
                       (or (= (length args) 1)
                           (admin-arg-coercible-p (second args) :int8)))
            (ereport "42883" "function ~a(~a) does not exist"
                     name (call-signature args)))
          (list :func name args :type :bool :typmod -1))
         ((member name '("pg_reload_conf" "pg_reload_code") :test #'string=)
          (when args
            (ereport "42883" "function ~a(~a) does not exist"
                     name (call-signature args)))
          (list :func name args :type :bool :typmod -1))
         (t
          (ereport "42883" "function ~a(~a) does not exist"
                   name (call-signature args))))))
    (t
     (ereport "XX000" "cannot analyze expression ~a" expr))))

(defun target-default-name (ast)
  (case (first ast)
    (:column
     (if (third ast) (third ast) (second ast)))
    (t "?column?")))

(defun analyze-target (target table)
  (let* ((ast (getf target :expr))
         (alias (getf target :alias))
         (resolved (analyze-expr ast table))
         (direct-column (and (eq (first resolved) :col) resolved))
         (type (expr-type resolved))
         (typmod (expr-typmod resolved))
         (name (or alias (target-default-name ast))))
    (list :expr resolved
          :name name
          :type type
          :typmod typmod
          :table-oid (if direct-column
                         (or (getf direct-column :table-oid) 0)
                         0)
          :attnum (if direct-column
                      (or (getf direct-column :attnum)
                          (second direct-column))
                      0))))

(defun select-column-target (column)
  (let ((expr (select-column-expr column)))
    (list :expr expr
          :name (select-column-name column)
          :type (select-column-type column)
          :typmod (select-column-typmod column)
          :table-oid (select-column-table-oid column)
          :attnum (select-column-attnum column))))

(defun analyze-targets (targets table)
  (loop for tg in targets
        if (getf tg :star)
          append (if (typep table 'select-scope)
                     (mapcar #'select-column-target
                             (select-scope-columns table))
                     (loop for col across (pg-class-columns table)
                           collect (list :expr
                                         (list :col (pg-column-attnum col)
                                               :type (pg-column-type col)
                                               :typmod (pg-column-typmod col)
                                               :table-oid (pg-class-oid table)
                                               :attnum (pg-column-attnum col))
                                         :name (pg-column-name col)
                                         :type (pg-column-type col)
                                         :typmod (pg-column-typmod col)
                                         :table-oid (pg-class-oid table)
                                         :attnum (pg-column-attnum col))))
        else
          collect (analyze-target tg table)))

(defun analyze-order (order table targets)
  (loop for item in order
        collect (let ((expr (getf item :expr)))
                  (if (and (eq (first expr) :const)
                           (integerp (second expr))
                           (<= 1 (second expr) (length targets)))
                      (cons (getf (nth (1- (second expr)) targets) :expr)
                            (getf item :desc))
                      (cons (analyze-expr expr table)
                            (getf item :desc))))))

;;; ---- evaluation ----

(defun true-p (v)
  (and v (not (eq v +null+))))

(defun compare-3way (a b)
  (cond
    ((and (numberp a) (numberp b))
     (cond ((< a b) -1) ((> a b) 1) (t 0)))
    ((and (stringp a) (stringp b))
     (cond ((string< a b) -1) ((string> a b) 1) (t 0)))
    (t
     (ereport "42883" "operator does not exist: ~a and ~a"
              (type-of a) (type-of b)))))

(defun sql-compare (op a b)
  (sql-strict-let ((left a) (right b))
    (let ((c (compare-3way left right)))
      (cond
        ((string= op "=") (zerop c))
        ((string= op "<>") (not (zerop c)))
        ((string= op "<") (minusp c))
        ((string= op "<=") (not (plusp c)))
        ((string= op ">") (plusp c))
        ((string= op ">=") (not (minusp c)))
        (t (ereport "XX000" "bad comparison operator ~a" op))))))

(defun sql-arith (op a b)
  (sql-strict-let ((left a) (right b))
    (unless (and (integerp left) (integerp right))
      (ereport "42883" "operator does not exist: ~a ~a ~a"
               (type-of left) op (type-of right)))
    (cond
      ((string= op "+") (+ left right))
      ((string= op "-") (- left right))
      ((string= op "*") (* left right))
      ((string= op "/")
       (when (zerop right)
         (ereport "22012" "division by zero"))
       (truncate left right))
      ((string= op "%")
       (when (zerop right)
         (ereport "22012" "division by zero"))
       (mod left right))
      (t (ereport "XX000" "bad arithmetic operator ~a" op)))))

(defun sql-3val-and (a b)
  (cond ((or (eq a nil) (eq b nil)) nil)
        ((and (eq a t) (eq b t)) t)
        (t +null+)))

(defun sql-3val-or (a b)
  (cond ((or (eq a t) (eq b t)) t)
        ((and (eq a nil) (eq b nil)) nil)
        (t +null+)))

(defun sql-3val-not (a)
  (cond ((eq a +null+) +null+)
        (a nil)
        (t t)))

;;; Admin-function argument coercion.  PostgreSQL resolves the calls against
;;; pg_proc (PID integer, timeout bigint) and the functions are not strict, so
;;; a NULL argument is 0 and a string literal is parsed as the target type.

(defun admin-pid-arg (value)
  (let ((n (cond
             ((eq value +null+) 0)
             ((integerp value) value)
             ((stringp value)
              (handler-case-nonfatal
               (parse-integer value :junk-allowed nil)
               (e
                (ereport "22P02"
                         "invalid input syntax for type integer: \"~a\""
                         value))))
             (t (ereport "22P02"
                         "invalid input syntax for type integer: \"~a\""
                         value)))))
    (unless (<= +int4-min+ n +int4-max+)
      (ereport "22003" "integer out of range"))
    n))

(defun admin-timeout-arg (value)
  (let ((n (cond
             ((eq value +null+) 0)
             ((integerp value) value)
             ((stringp value)
              (handler-case-nonfatal
               (parse-integer value :junk-allowed nil)
               (e
                (ereport "22P02"
                         "invalid input syntax for type bigint: \"~a\""
                         value))))
             (t (ereport "22P02"
                         "invalid input syntax for type bigint: \"~a\""
                         value)))))
    (unless (<= +int8-min+ n +int8-max+)
      (ereport "22003" "bigint out of range"))
    n))

(defun eval-func (name args)
  (cond
    ((string= name "version")
     "PostgreSQL 16.0 (pg-lisp 0.1)")
    ((string= name "pg_lisp_runtime")
     (format nil "~a ~a" (lisp-implementation-type)
             (lisp-implementation-version)))
    ((string= name "current_database")
     (backend-database *backend*))
    ((string= name "current_schema") "public")
    ((member name '("current_user" "session_user" "user") :test #'string=)
     "postgres")
    ((string= name "current_catalog")
     (backend-database *backend*))
    ((string= name "pg_backend_pid")
     (if *backend* (backend-pid *backend*) 0))
    ((string= name "pg_cancel_backend")
     (admin-cancel-backend (admin-pid-arg (first args))))
    ((string= name "pg_terminate_backend")
     (admin-terminate-backend (admin-pid-arg (first args))
                              (if (rest args)
                                  (admin-timeout-arg (second args))
                                  0)))
    ((string= name "pg_reload_conf")
     (admin-reload-conf))
    ((string= name "pg_reload_code")
     (admin-reload-code))
    ((member name '("length" "char_length" "character_length")
             :test #'string=)
     (sql-strict-let ((value (first args)))
       (length value)))
    (t
     (ereport "42883" "function ~a(...) does not exist" name))))

(defun eval-expr (expr env)
  (case (first expr)
    (:const (second expr))
    (:bool (second expr))
    (:col
     (unless env
       (ereport "42703" "column does not exist in this context"))
     (aref env (1- (second expr))))
    (:join-coalesce
     (let ((left (eval-expr (second expr) env)))
       (if (eq left +null+)
           (eval-expr (third expr) env)
           left)))
    (:join-relabel
     (eval-expr (second expr) env))
    (:binop
     (let ((op (second expr))
           (l (eval-expr (third expr) env))
           (r (eval-expr (fourth expr) env)))
       (if (member op '("=" "<>" "<" "<=" ">" ">=") :test #'string=)
           (sql-compare op l r)
           (sql-arith op l r))))
    (:unary
     (sql-strict-let ((value (eval-expr (third expr) env)))
       (cond
         ((string= (second expr) "-") (- value))
         (t (ereport "42883" "unsupported unary operator ~a"
                     (second expr))))))
    (:and (sql-3val-and (eval-expr (second expr) env)
                        (eval-expr (third expr) env)))
    (:or (sql-3val-or (eval-expr (second expr) env)
                      (eval-expr (third expr) env)))
    (:not (sql-3val-not (eval-expr (second expr) env)))
    (:is-null
     (let ((v (eval-expr (second expr) env)))
       (if (third expr)
           (not (eq v +null+))
           (eq v +null+))))
    (:is-bool
     (let ((v (eval-expr (second expr) env))
           (want (fourth expr))
           (neg (third expr)))
       (let ((result (cond ((eq v +null+) nil)
                           ((eq v t) (eq want t))
                           (t (eq want nil)))))
         (if neg (not result) result))))
    (:func
     (eval-func (second expr)
                (mapcar (lambda (a) (eval-expr a env))
                        (third expr))))
    (t
     (ereport "XX000" "cannot evaluate expression ~a" expr))))

(defun value->text (v type)
  (cond
    ((eq v +null+) nil)
    ((eq type :bool) (if v "t" "f"))
    ((stringp v) v)
    (t (princ-to-string v))))
