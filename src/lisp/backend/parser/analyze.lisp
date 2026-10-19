;;;; analyze.lisp -- name resolution and expression evaluation.

(in-package #:pg)

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
     (let ((name (if (third expr) (third expr) (second expr))))
       (unless table
         (ereport "42703" "column \"~a\" does not exist" name))
       (let ((col (find-column table name)))
         (unless col
           (ereport "42703" "column \"~a\" does not exist" name))
         (list :col (pg-column-attnum col)
               :type (pg-column-type col)
               :typmod (pg-column-typmod col)
               :notnull (pg-column-notnull col)))))
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
         ((member name '("version" "current_database" "current_schema"
                         "current_catalog" "current_user" "session_user" "user")
                  :test #'string=)
          (when args
            (ereport "42883" "function ~a() does not exist" name))
          (list :func name args :type :text :typmod -1))
         ((member name '("length" "char_length" "character_length")
                  :test #'string=)
          (unless (= (length args) 1)
            (ereport "42883" "function ~a() does not exist" name))
          (list :func name args :type :int4 :typmod -1))
         ((string= name "pg_backend_pid")
          (list :func name args :type :int4 :typmod -1))
         (t
          (ereport "42883" "function ~a(...) does not exist" name)))))
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
         (type (expr-type resolved))
         (typmod (expr-typmod resolved))
         (direct-col-p (eq (first resolved) :col))
         (name (or alias (target-default-name ast))))
    (list :expr resolved
          :name name
          :type type
          :typmod typmod
          :table-oid (if (and table direct-col-p) (pg-class-oid table) 0)
          :attnum (if direct-col-p (second resolved) 0))))

(defun analyze-targets (targets table)
  (loop for tg in targets
        if (getf tg :star)
          append (loop for col across (pg-class-columns table)
                       collect (list :expr (list :col (pg-column-attnum col)
                                                 :type (pg-column-type col)
                                                 :typmod (pg-column-typmod col))
                                     :name (pg-column-name col)
                                     :type (pg-column-type col)
                                     :typmod (pg-column-typmod col)
                                     :table-oid (pg-class-oid table)
                                     :attnum (pg-column-attnum col)))
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
  (if (or (eq a +null+) (eq b +null+))
      +null+
      (let ((c (compare-3way a b)))
        (cond
          ((string= op "=") (zerop c))
          ((string= op "<>") (not (zerop c)))
          ((string= op "<") (minusp c))
          ((string= op "<=") (not (plusp c)))
          ((string= op ">") (plusp c))
          ((string= op ">=") (not (minusp c)))
          (t (ereport "XX000" "bad comparison operator ~a" op))))))

(defun sql-arith (op a b)
  (if (or (eq a +null+) (eq b +null+))
      +null+
      (progn
        (unless (and (integerp a) (integerp b))
          (ereport "42883" "operator does not exist: ~a ~a ~a"
                   (type-of a) op (type-of b)))
        (cond
          ((string= op "+") (+ a b))
          ((string= op "-") (- a b))
          ((string= op "*") (* a b))
          ((string= op "/")
           (when (zerop b)
             (ereport "22012" "division by zero"))
           (truncate a b))
          ((string= op "%")
           (when (zerop b)
             (ereport "22012" "division by zero"))
           (mod a b))
          (t (ereport "XX000" "bad arithmetic operator ~a" op))))))

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

(defun eval-func (name args)
  (cond
    ((string= name "version")
     "PostgreSQL 16.0 (pg-lisp 0.1)")
    ((string= name "current_database")
     (backend-database *backend*))
    ((string= name "current_schema") "public")
    ((member name '("current_user" "session_user" "user") :test #'string=)
     "postgres")
    ((string= name "current_catalog")
     (backend-database *backend*))
    ((string= name "pg_backend_pid") 1)
    ((member name '("length" "char_length" "character_length")
             :test #'string=)
     (let ((v (first args)))
       (if (eq v +null+) +null+ (length v))))
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
    (:binop
     (let ((op (second expr))
           (l (eval-expr (third expr) env))
           (r (eval-expr (fourth expr) env)))
       (if (member op '("=" "<>" "<" "<=" ">" ">=") :test #'string=)
           (sql-compare op l r)
           (sql-arith op l r))))
    (:unary
     (let ((v (eval-expr (third expr) env)))
       (if (eq v +null+)
           +null+
           (cond
             ((string= (second expr) "-") (- v))
             (t (ereport "42883" "unsupported unary operator ~a"
                         (second expr)))))))
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
