;;;; analyze.lisp -- name resolution and expression evaluation.

(in-package #:pg)

(defstruct select-relation
  name
  alias
  table
  origins
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
      (sql-relation-base-name (select-relation-name relation))))

(defun resolve-select-column (scope qualifier name)
  (let* ((relation
           (when qualifier
             (or (find qualifier (select-scope-relations scope)
                       :key #'select-relation-qualifier
                       :test (lambda (a b) (and b (string= a b))))
                 (ereport "42P01" "missing FROM-clause entry for table \"~a\"" qualifier))))
         (columns (if qualifier (select-relation-columns relation)
                      (select-scope-columns scope)))
         (matches (remove-if-not
                   (lambda (column) (string= name (select-column-name column)))
                   columns)))
    (cond ((null matches) (ereport "42703" "column \"~a\" does not exist" name))
          ((rest matches) (ereport "42702" "column reference \"~a\" is ambiguous" name))
          (t (select-column-expr (first matches))))))

(defmacro sql-strict-let (bindings &body body)
  "Evaluate all BINDINGS once, returning SQL NULL if any value is NULL."
  `(let* ,bindings
     (if (or ,@(mapcar (lambda (binding)
                         `(eq ,(first binding) +null+))
                       bindings))
         +null+
         (progn ,@body))))

(defparameter +sql-expression-types+
  '((:bool 16 1) (:char 18 1) (:name 19 64) (:int8 20 8)
    (:int2 21 2) (:int2vector 22 -1) (:int4 23 4) (:text 25 -1) (:varchar 1043 -1) (:text-array 1009 -1) (:oid 26 4) (:pg-node-tree 194 -1) (:record 2249 -1) (:pg-lsn 3220 8)))

(defun sql-expression-type-description (type)
  (or (assoc type +sql-expression-types+)
      (ereport "0A000" "unsupported expression type ~a" type)))

(defun sql-expression-type-length (type)
  (third (sql-expression-type-description type)))

(defun type-oid (type)
  (second (sql-expression-type-description type)))

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
    (:int2 "smallint")
    (:int4 "integer")
    (:int8 "bigint")
    (:varchar "character varying")
    (:bool "boolean")
    (:text-array "text[]")
    (:int2vector "int2vector")
    (:char "\"char\"")
    (:name "name")
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

(defun builtin-function-name (name schema)
  "Resolve explicit qualification without splitting literal identifier contents."
  (when (and schema
             (or (not (string= schema "pg_catalog"))
                 (member name '("user" "current_catalog") :test #'string=)))
    (ereport "42883" "function ~a.~a(...) does not exist" schema name))
  name)

(defvar *select-set-functions-allowed-p* nil)

(defvar *inside-aggregate-input-p* nil)

(defun coerce-catalog-oid-argument (argument name)
  (unless (or (in-unknown-constant-p argument)
              (member (expr-type argument) '(:int2 :int4 :int8 :oid)))
    (ereport "42883" "function ~a(~a) does not exist" name (typed-arg-name argument)))
  (coerce-numeric-expression-to-oid (coerce-in-constant argument :oid)))

(defun analyze-metadata-function (name arguments)
  (cond
    ((string= name "array_length")
     (unless (and (= 2 (length arguments))
                  (member (expr-type (first arguments)) '(:int2vector :text-array)))
       (ereport "42883" "function ~a(~a) does not exist" name (call-signature arguments)))
     (let ((dimension (coerce-in-constant (second arguments) :int4)))
       (unless (member (expr-type dimension) '(:int2 :int4))
         (ereport "42883" "array_length dimension must be integer"))
       (when (eq (expr-type dimension) :int2)
         (setf dimension (list :cast dimension :type :int4 :typmod -1)))
       (list :array-length (first arguments) dimension :type :int4 :typmod -1)))
    ((string= name "pg_get_replica_identity_index")
     (unless (= 1 (length arguments))
       (ereport "42883" "function ~a requires one relation OID" name))
     (list :func name (list (coerce-catalog-oid-argument (first arguments) name))
           :type :oid :typmod -1))
    ((string= name "pg_get_expr")
     (unless (= 2 (length arguments))
       (ereport "42883" "function ~a requires pg_node_tree and OID" name))
     (let ((node (coerce-in-constant (first arguments) :pg-node-tree)))
       (unless (eq (expr-type node) :pg-node-tree)
         (ereport "42883" "pg_get_expr requires pg_node_tree"))
       (list :func name (list node (coerce-catalog-oid-argument (second arguments) name))
             :type :text :typmod -1)))))

(defun analyze-function-expression (expr table)
  "Resolve a supported scalar or administrative function call."
  (let* ((name (builtin-function-name (second expr) (getf (cdddr expr) :schema)))
         (args (mapcar (lambda (arg) (analyze-expr arg table)) (third expr))))
    ;; C ParseFuncOrColumn clears explicit VARIADIC for a nonvariadic match.
    (when (string= name "pg_get_publication_tables")
      (return-from analyze-function-expression
        (analyze-publication-table-call name args (getf (cdddr expr) :variadic))))
    (cond
      ((member name '("array_length" "pg_get_expr" "pg_get_replica_identity_index") :test #'string=)
       (analyze-metadata-function name args))
      ((member name '("version" "pg_lisp_runtime" "pg_current_wal_lsn"
                      "pg_lisp_cluster_identity"
                      "current_database" "current_schema"
                      "current_catalog" "current_user" "session_user" "user")
               :test #'string=)
       (when args
         (ereport "42883" "function ~a(~a) does not exist"
                  name (call-signature args)))
       (list :func name args
             :type (cond ((string= name "pg_current_wal_lsn") :pg-lsn)
                         ((string= name "pg_lisp_cluster_identity") :varchar)
                         (t :text))
             :typmod -1))
      ((string= name "pg_lisp_acquire_subscription")
       (unless (and (= (length args) 1) (eq (expr-type (first args)) :text))
         (ereport "42883" "pg_lisp_acquire_subscription requires one text key"))
       (list :func name args :type :bool :typmod -1))
      ((string= name "pg_lisp_replica_identity_columns")
       (unless (= (length args) 1)
         (ereport "42883" "function ~a(~a) does not exist"
                  name (call-signature args)))
       (list :func name args :type :varchar :typmod -1))
      ((member name '("pg_lisp_publication_definition"
                      "pg_lisp_table_definition"
                      "pg_lisp_sequence_state")
               :test #'string=)
       (unless (= (length args) 1)
         (ereport "42883" "function ~a(~a) does not exist"
                  name (call-signature args)))
       (list :func name args :type :varchar :typmod -1))
      ((string= name "pg_lisp_replication_bootstrap")
       (unless (null args)
         (ereport "42883" "function ~a(~a) does not exist"
                  name (call-signature args)))
       (list :func name args :type :varchar :typmod -1))
      ((member name '("nextval" "currval") :test #'string=)
       (unless (and (= (length args) 1)
                    (eq (expr-type (first args)) :text))
         (ereport "42883" "function ~a(~a) does not exist"
                  name (call-signature args)))
       (list :func name args :type :int8 :typmod -1))
      ((string= name "set_config")
       (unless (and (= (length args) 3)
                    (eq (expr-type (first args)) :text)
                    (eq (expr-type (second args)) :text)
                    (or (eq (expr-type (third args)) :bool)
                        (equal (third args) (list :const +null+ :type :text :typmod -1))))
         (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
       (list :func name args :type :text :typmod -1))
      ((string= name "setval")
       (unless (and (member (length args) '(2 3))
                    (eq (expr-type (first args)) :text)
                    (member (expr-type (second args)) '(:int4 :int8))
                    (or (= (length args) 2)
                        (eq (expr-type (third args)) :bool)))
         (ereport "42883" "function ~a(~a) does not exist"
                  name (call-signature args)))
       (list :func name args :type :int8 :typmod -1))
      ((member name '("length" "char_length" "character_length") :test #'string=)
       (unless (and (= (length args) 1)
                    (member (expr-type (first args)) '(:text :varchar :name :char)))
         (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
       (when (eq (expr-type (first args)) :char)
         (setf args (list (list :cast (first args) :type :text :typmod -1))))
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

(defun expression-contains-column-p (form)
  "Detect row references through every analyzed expression, including IN lists."
  (and (consp form)
       (or (eq (first form) :col)
           (some #'expression-contains-column-p form))))

(defun in-unknown-constant-p (expr)
  (and (eq (first expr) :const)
       (not (getf (cddr expr) :explicit-cast))
       (or (stringp (second expr)) (eq (second expr) +null+))))

(defun sql-name-input (text)
  "Match C namein's UTF8-safe NAMEDATALEN-1 clipping."
  (if (<= (length (string->bytes text)) 63)
      text
      (subseq text 0
              (loop with bytes = 0
                    for index from 0 below (length text)
                    for width = (length (string->bytes (subseq text index (1+ index))))
                    when (> (+ bytes width) 63) return index
                      do (incf bytes width)
                    finally (return (length text))))))

(defun coerce-in-constant (expr type)
  "Coerce unknown IN literals through the shared SQL type input functions."
  (if (in-unknown-constant-p expr)
      (list :const (sql-cast-value (second expr) :text type -1) :type type :typmod -1)
      expr))

(defun coerce-numeric-expression-to-oid (expression)
  "Preserve C's int4 binary cast and int8 checked conversion in OID operators."
  (let ((source (expr-type expression)))
    (if (member source '(:int2 :int4 :int8))
        (if (eq (first expression) :const)
            (list :const (sql-cast-value (second expression) source :oid -1)
                  :type :oid :typmod -1 :explicit-cast t)
            (list :cast expression :type :oid :typmod -1))
        expression)))

(defun coerce-in-operands (expressions)
  "Choose a common supported IN type, keeping unknown literals context-sensitive."
  (let* ((types (mapcar #'expr-type (remove-if #'in-unknown-constant-p expressions)))
         (type (or (first types) :text))
         (category (cond ((member type '(:int2 :int4 :int8 :oid)) :numeric)
                         ((member type '(:text :varchar :name)) :string)
                         ((eq type :bool) :bool)
                         (t type))))
    (unless (every (lambda (item)
                     (case category
                       (:numeric (member item '(:int2 :int4 :int8 :oid)))
                       (:string (member item '(:text :varchar :name)))
                       (:bool (eq item :bool))
                       (otherwise (eq item type))))
                   types)
      (return-from coerce-in-operands nil))
    (when (eq category :numeric)
      (cond ((member :oid types) (setf type :oid))
            ((member :int8 types) (setf type :int8))
            ((member :int4 types) (setf type :int4))))
    (when (and (eq category :string) (member :text types)) (setf type :text))
    (mapcar (lambda (expr)
              (let ((coerced (coerce-in-constant expr type)))
                (if (eq type :oid) (coerce-numeric-expression-to-oid coerced) coerced)))
            expressions)))

(defun analyze-comparison-operands (left right)
  "Coerce unknown constants, and resolve new catalog types before value comparison."
  (cond
    ((and (in-unknown-constant-p left) (not (in-unknown-constant-p right)))
     (setf left (coerce-in-constant left (expr-type right))))
    ((and (in-unknown-constant-p right) (not (in-unknown-constant-p left)))
     (setf right (coerce-in-constant right (expr-type left)))))
  (when (and (or (eq (expr-type left) :oid) (eq (expr-type right) :oid))
             (every (lambda (expression) (member (expr-type expression) '(:int2 :int4 :int8 :oid)))
                    (list left right)))
    (setf left (coerce-numeric-expression-to-oid left)
          right (coerce-numeric-expression-to-oid right)))
  (let ((left-type (expr-type left)) (right-type (expr-type right)))
    (when (or (member left-type '(:char :int2vector))
              (member right-type '(:char :int2vector)))
      (cond
        ((eq left-type right-type))
        ((and (member left-type '(:char :text :varchar :name))
              (member right-type '(:char :text :varchar :name)))
         (setf left (list :cast left :type :text :typmod -1)
               right (list :cast right :type :text :typmod -1)))
        (t (ereport "42883" "operator does not exist for ~a and ~a" left-type right-type)))))
  (let ((left-type (expr-type left)) (right-type (expr-type right)))
    (unless (or (eq left-type right-type)
                (and (member left-type '(:int2 :int4 :int8)) (member right-type '(:int2 :int4 :int8)))
                (and (member left-type '(:text :varchar :name))
                     (member right-type '(:text :varchar :name))))
      (ereport "42883" "operator does not exist for ~a and ~a" left-type right-type)))
  (list left right))

(defun analyze-in-expression (expr table)
  "Match C transformAExprIn grouping, common types, and per-comparison coercion."
  (let* ((left (analyze-expr (second expr) table))
         (items (mapcar (lambda (item) (analyze-expr item table)) (third expr)))
         (nonvars (remove-if #'expression-contains-column-p items))
         (vars (remove-if-not #'expression-contains-column-p items))
         (array (when (> (length nonvars) 1)
                  (coerce-in-operands (cons left nonvars))))
         (comparisons
           (mapcar (lambda (item) (analyze-comparison-operands left item))
                   (if array vars items))))
    (list :in left items (fourth expr) :type :bool :typmod -1
          :array-left (first array) :array-items (rest array)
          :comparisons comparisons)))

(defun sql-cast-allowed-p (source target)
  "Supported explicit casts from pg_cast and CoerceViaIO's string rules."
  (or (eq source target)
      (member target '(:text :varchar :name))
      (member source '(:text :varchar :name))
      (and (member source '(:int2 :int4 :int8 :oid))
           (member target '(:int2 :int4 :int8 :oid)))
      (and (eq source :int4) (member target '(:bool :char)))
      (and (eq target :int4) (member source '(:bool :char)))))

(defun sql-cast-integer-input (text type)
  "Read signed integer input with C pg_strtoint's radix and underscore rules."
  (let* ((input (string-trim (mapcar #'code-char '(9 10 11 12 13 32)) text))
         (size (length input)) (position 0) (sign 1) (radix 10)
         (prefixed nil) (digits 0) (number 0))
    (labels ((invalid () (ereport "22P02" "invalid input syntax for integer: ~s" text))
             (digit-at (index)
               (when (< index size)
                 (let ((character (char input index)))
                   (when (< (char-code character) 128)
                     (digit-char-p character radix))))))
      (when (and (< position size) (find (char input position) "+-"))
        (when (char= (char input position) #\-) (setf sign -1))
        (incf position))
      (when (and (< (1+ position) size) (char= (char input position) #\0))
        (let ((base (case (char-downcase (char input (1+ position)))
                      (#\x 16) (#\o 8) (#\b 2))))
          (when base (setf radix base prefixed t) (incf position 2))))
      (loop while (< position size)
            for character = (char input position)
            for digit = (digit-at position)
            do (cond
                 (digit
                  (when (> number (floor (if (eq type :int4)
                                             (- +int4-min+) (- +int8-min+)) radix))
                    (ereport "22003" "integer out of range"))
                  (setf number (+ (* number radix) digit))
                  (incf digits))
                 ((char= character #\_)
                  (unless (and (or prefixed (plusp digits))
                               (digit-at (1+ position)))
                    (invalid)))
                 (t (invalid)))
               (incf position))
      (when (zerop digits) (invalid))
      (setf number (* sign number))
      (unless (if (eq type :int4) (<= +int4-min+ number +int4-max+)
                  (<= +int8-min+ number +int8-max+))
        (ereport "22003" "integer out of range"))
      number)))

(defun sql-cast-boolean-input (text)
  (let* ((input (string-downcase
                 (string-trim (mapcar #'code-char '(9 10 11 12 13 32)) text)))
         (yes (member input '("true" "tru" "tr" "t" "yes" "ye" "y" "on" "1") :test #'string=))
         (no (member input '("false" "fals" "fal" "fa" "f" "no" "n" "off" "of" "0") :test #'string=)))
    (unless (or yes no) (ereport "22P02" "invalid input syntax for boolean: ~s" text))
    (not (null yes))))

(defun sql-char-input (text)
  "Internal char is one unsigned byte; preserve C charin's octal notation."
  (cond
    ((and (= 4 (length text)) (char= (char text 0) (code-char 92))
          (every (lambda (character) (digit-char-p character 8)) (subseq text 1)))
     (mod (parse-integer text :start 1 :radix 8) 256))
    ((zerop (length text)) 0)
    (t (aref (string->bytes text) 0))))

(defun sql-char-output (value)
  (cond ((zerop value) "")
        ((< value 128) (string (code-char value)))
        (t (concatenate 'string (string (code-char 92)) (format nil "~3,'0o" value)))))

(defun sql-int2vector-input (text)
  "Follow int2vectorin: decimal int16 values with a literal space after each."
  (let ((position 0) (size (length text)) (values nil))
    (labels ((digit-at (index)
               (and (< index size) (char<= #\0 (char text index) #\9)
                    (digit-char-p (char text index))))
             (invalid () (ereport "22P02" "invalid input syntax for smallint: ~s" text)))
      (loop
        (loop while (and (< position size)
                         (find (char text position) (mapcar #'code-char '(9 10 11 12 13 32))))
              do (incf position))
        (when (= position size) (return (coerce (nreverse values) 'vector)))
        (let ((sign 1) (number 0) (digits 0))
          (when (find (char text position) "+-")
            (when (char= (char text position) #\-) (setf sign -1))
            (incf position))
          (loop for digit = (digit-at position) while digit
                do (setf number (+ (* number 10) digit))
                   (incf position) (incf digits))
          (when (zerop digits) (invalid))
          (setf number (* sign number))
          (unless (<= -32768 number 32767) (ereport "22003" "smallint out of range"))
          (when (and (< position size) (not (char= (char text position) #\Space)))
            (invalid))
          (push number values))))))

(defun sql-cast-string-value (value source target)
  (if (eq source :bool)
      (if (eq target :name) (if value "t" "f") (if value "true" "false"))
      (value->text value source)))

(defun sql-pg-lsn-input (text)
  "Follow pg_parse_lsn: exactly two components of one to eight ASCII hex digits."
  (let ((slash (position #\/ text)))
    (unless (and slash (<= 1 slash 8) (<= 1 (- (length text) slash 1) 8)
                 (loop for character across text for position from 0
                       always (or (= position slash)
                                  (find character "0123456789abcdefABCDEF"))))
      (ereport "22P02" "invalid input syntax for type pg_lsn: ~s" text))
    (logior (ash (parse-integer text :end slash :radix 16) 32)
            (parse-integer text :start (1+ slash) :radix 16))))

(defun sql-oid-input (text)
  "Match oidin's strtoul(base 0), including signed int32 compatibility."
  (labels ((space-p (character) (member (char-code character) '(9 10 11 12 13 32)))
           (ascii-digit (character radix)
             (and (< (char-code character) 128) (digit-char-p character radix))))
    (let* ((length (length text))
           (position (or (position-if-not #'space-p text) length))
           (sign 1) (radix 10) (digits 0) (value 0))
      (when (and (< position length) (find (char text position) "+-"))
        (when (char= (char text position) #\-) (setf sign -1))
        (incf position))
      (when (and (< position length) (char= (char text position) #\0))
        (setf radix 8)
        (when (< (+ position 2) length)
          (let ((prefix-radix (case (char text (1+ position))
                                ((#\x #\X) 16) ((#\b #\B) 2))))
            (when (and prefix-radix (ascii-digit (char text (+ position 2)) prefix-radix))
              (setf radix prefix-radix)
              (incf position 2)))))
      (loop while (< position length)
            for digit = (ascii-digit (char text position) radix)
            while digit
            do (when (> value (floor (- #xffffffffffffffff digit) radix))
                 (ereport "22003" "value ~s is out of range for type oid" text))
               (setf value (+ (* value radix) digit))
               (incf position) (incf digits))
      (when (> value #xffffffffffffffff)
        (ereport "22003" "value ~s is out of range for type oid" text))
      (unless (and (plusp digits) (every #'space-p (subseq text position)))
        (ereport "22P02" "invalid input syntax for type oid: ~s" text))
      (let* ((converted (mod (* sign value) (ash 1 64)))
             (result (logand converted #xffffffff))
             (signed (if (>= result #x80000000) (- result (ash 1 32)) result)))
        (unless (or (= converted result) (= converted (mod signed (ash 1 64))))
          (ereport "22003" "value ~s is out of range for type oid" text))
        result))))

(defun sql-cast-value (value source target typmod)
  (when (eq value +null+) (return-from sql-cast-value +null+))
  (when (eq target :int2)
    (let ((number (sql-cast-value value source :int4 -1)))
      (unless (<= -32768 number 32767)
        (ereport "22003" "smallint out of range"))
      (return-from sql-cast-value number)))
  (when (eq source :int2) (setf source :int4))
  (when (and (member target '(:int4 :int8)) (eq source target))
    (unless (if (eq target :int4) (<= +int4-min+ value +int4-max+)
                (<= +int8-min+ value +int8-max+))
      (ereport "22003" "integer out of range")))
  (cond
    ((member target '(:text :varchar :name))
     (let ((text (sql-cast-string-value value source target)))
       (cond ((eq target :name) (sql-name-input text))
             ((and (eq target :varchar) (>= typmod 4))
              (subseq text 0 (min (length text) (- typmod 4))))
             (t text))))
    ((eq source target) value)
    ((and (eq target :oid) (member source '(:int4 :int8)))
     (when (and (eq source :int8) (not (<= 0 value #xffffffff)))
       (ereport "22003" "OID out of range"))
     (logand value #xffffffff))
    ((member source '(:text :varchar :name))
     (case target
       ((:int4 :int8) (sql-cast-integer-input value target))
       (:bool (sql-cast-boolean-input value))
       (:char (sql-char-input value))
       (:int2vector (sql-int2vector-input value))
       (:oid (sql-oid-input value))
       (:pg-lsn (sql-pg-lsn-input value))
       (otherwise (ereport "0A000" "text input for ~a is not implemented" target))))
    ((eq target :bool) (not (zerop value)))
    ((eq source :bool) (if value 1 0))
    ((eq target :char)
     (unless (<= -128 value 127) (ereport "22003" "\"char\" out of range"))
     (mod value 256))
    ((eq source :char) (if (>= value 128) (- value 256) value))
    ((and (eq source :oid) (eq target :int4))
     (if (>= value #x80000000) (- value (ash 1 32)) value))
    ((eq target :int4)
     (unless (<= +int4-min+ value +int4-max+) (ereport "22003" "integer out of range"))
     value)
    (t value)))

(defun analyze-cast-expression (expression scope)
  (let* ((raw (second expression))
         (target (third expression)) (typmod (fourth expression))
         (argument
           (if (and (eq target :text-array) (eq (first raw) :array-literal)
                    (null (second raw)))
               (list :array nil :type :text-array :typmod -1)
               (analyze-expr raw scope)))
         (source (expr-type argument))
         (unknown-literal (and (eq (first raw) :const)
                               (or (stringp (second raw)) (eq (second raw) +null+)))))
    (sql-expression-type-description target)
    (unless (or unknown-literal (sql-cast-allowed-p source target))
      (ereport "42846" "cannot cast type ~a to ~a" source target))
    (cond
      (unknown-literal
       (list :const (sql-cast-value (second raw) source target typmod) :explicit-cast t
             :type target :typmod typmod))
      ((and (eq source target) (= typmod -1))
       (when (and (eq (first argument) :const) (member target '(:int2 :int4 :int8)))
         (sql-cast-value (second argument) source target typmod))
       argument)
      (t (list :cast argument :type target :typmod typmod)))))

(defun analyze-binop-expression (expression scope)
  (let* ((operator (second expression))
         (left (analyze-expr (third expression) scope))
         (right (analyze-expr (fourth expression) scope)))
    (if (member operator '("=" "<>" "<" "<=" ">" ">=") :test #'string=)
        (let ((operands (analyze-comparison-operands left right)))
          (list :binop operator (first operands) (second operands) :type :bool :typmod -1))
        (progn
          (when (or (eq (expr-type left) :pg-lsn) (eq (expr-type right) :pg-lsn))
            (ereport "0A000" "pg_lsn arithmetic is not implemented"))
          (when (or (member (expr-type left) '(:char :int2vector :text-array :oid))
                    (member (expr-type right) '(:char :int2vector :text-array :oid)))
            (ereport "42883" "operator ~a does not exist for ~a and ~a"
                     operator (expr-type left) (expr-type right)))
          (list :binop operator left right
                :type (cond ((or (eq (expr-type left) :int8) (eq (expr-type right) :int8)) :int8)
                            ((and (eq (expr-type left) :int2) (eq (expr-type right) :int2)) :int2)
                            (t :int4))
                :typmod -1)))))

(defun coerce-common-expression (expression type)
  (cond
    ((in-unknown-constant-p expression) (coerce-in-constant expression type))
    ((eq type (expr-type expression)) expression)
    ((sql-implicit-coercion-p (expr-type expression) type)
     (list :cast expression :type type :typmod -1))
    (t (ereport "42846" "cannot implicitly convert ~a to ~a" (expr-type expression) type))))

(defun common-expression-type (expressions context)
  "Choose a common type while deferring all-unknown groups until the final choice."
  (let ((known (remove-if #'in-unknown-constant-p expressions)))
    (unless known (return-from common-expression-type :text))
    (let ((target (list :expr (first known) :type (expr-type (first known))
                        :typmod (expr-typmod (first known)))))
      (dolist (expression (rest known))
        (multiple-value-bind (type typmod)
            (union-common-type target
                               (list :expr expression :type (expr-type expression)
                                     :typmod (expr-typmod expression))
                               context)
          (setf target (list :expr (list :col 0 :type type :typmod typmod)
                             :type type :typmod typmod))))
      (getf target :type))))

(defun analyze-case-expression (expression scope)
  (let* ((branches
           (mapcar
            (lambda (branch)
              (let ((condition (coerce-in-constant (analyze-expr (first branch) scope) :bool))
                    (value (analyze-expr (second branch) scope)))
                (unless (eq (expr-type condition) :bool)
                  (ereport "42804" "CASE condition must be boolean"))
                (list condition value)))
            (second expression)))
         (otherwise (analyze-expr (third expression) scope))
         (values (cons otherwise (mapcar #'second branches)))
         (type (common-expression-type values "CASE"))
         (coerced (mapcar (lambda (value) (coerce-common-expression value type)) values)))
    (when (analyzed-expression-contains-set-p (list branches otherwise))
      (ereport "0A000" "set-returning functions are not allowed in CASE"))
    (list :case
          (loop for branch in branches for value in (rest coerced)
                collect (list (first branch) value))
          (first coerced) :type type
          :typmod (if (every (lambda (value) (= (expr-typmod value)
                                                (expr-typmod (first coerced)))) coerced)
                      (expr-typmod (first coerced)) -1))))

(defun analyze-array-literal (expression scope)
  (let ((items (mapcar (lambda (item) (analyze-expr item scope)) (second expression))))
    (unless items (ereport "42P18" "cannot determine type of empty array"))
    (unless (every (lambda (item) (eq (expr-type item) :text)) items)
      (ereport "0A000" "ARRAY constructors currently support text elements"))
    (list :array items :type :text-array :typmod -1)))

(defun analyze-any-expression (expression scope)
  (let* ((left (analyze-expr (third expression) scope))
         (array (analyze-expr (fourth expression) scope))
         (element (case (expr-type array)
                    (:text-array :text) (:int2vector :int2))))
    (when (and (in-unknown-constant-p array) (eq (second array) +null+))
      (return-from analyze-any-expression
        (list :any (second expression) left array :type :bool :typmod -1)))
    (unless element (ereport "42809" "ANY requires a supported array argument"))
    (let ((operands
            (analyze-comparison-operands
             left (list :const +null+ :type element :typmod -1 :explicit-cast t))))
      (list :any (second expression) (first operands) array
            :element-source element :element-target (expr-type (second operands))
            :type :bool :typmod -1))))

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
    (:case (analyze-case-expression expr table))
    (:array-literal (analyze-array-literal expr table))
    (:any (analyze-any-expression expr table))
    (:cast (analyze-cast-expression expr table))
    (:record-field (analyze-record-field expr table))
    (:bool
     (list :bool (second expr) :type :bool :typmod -1))
    (:column
     (let ((name (if (third expr) (third expr) (second expr)))
           (qualifier (and (third expr) (second expr))))
       (cond
         ((typep table 'select-scope)
          (resolve-select-column table qualifier name))
         (t
          (when (and (null table) qualifier)
            (ereport "42P01" "missing FROM-clause entry for table \"~a\"" qualifier))
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
    (:binop (analyze-binop-expression expr table))
    (:unary
     (let ((e (analyze-expr (third expr) table)))
       (when (member (expr-type e) '(:char :int2vector :text-array :oid :pg-lsn))
         (ereport "42883" "unary operator does not exist for ~a" (expr-type e)))
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
    (:in (analyze-in-expression expr table))
    (:is-null
     (list :is-null (analyze-expr (second expr) table) (third expr)
           :type :bool :typmod -1))
    (:is-bool
     (list :is-bool (analyze-expr (second expr) table) (third expr)
           (fourth expr) :type :bool :typmod -1))
    (:func (if (raw-aggregate-kind expr)
               (analyze-aggregate-call expr table)
               (analyze-function-expression expr table)))
    (:count-star
     (analyze-aggregate-call expr table))
    (t
     (ereport "XX000" "cannot analyze expression ~a" expr))))

(defun cast-target-default-name (ast)
  "A cast supplies a weak type label; a column/function label takes precedence."
  (let ((argument (second ast)))
    (loop while (eq (first argument) :cast) do (setf argument (second argument)))
    (case (first argument)
      (:func (second argument))
      ((:column :count-star) (target-default-name argument))
      (t (if (eq (third ast) :pg-lsn) "pg_lsn"
             (string-downcase (symbol-name (third ast))))))))

(defun target-default-name (ast)
  (case (first ast)
    (:column
     (if (third ast) (third ast) (second ast)))
    (:count-star "count")
    (:func (second ast))
    (:record-field (third ast))
    (:cast (cast-target-default-name ast))
    (t "?column?")))

(defun analyze-target (target table)
  (let* ((ast (getf target :expr))
         (alias (getf target :alias))
         (resolved (let ((*select-set-functions-allowed-p* t)) (analyze-expr ast table)))
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

(defun sql-composite-call-fields (raw)
  (unless (and (eq (first raw) :func)
               (string= (builtin-function-name (second raw) (getf (cdddr raw) :schema))
                        "pg_get_publication_tables"))
    (ereport "42809" "expression is not a supported composite type"))
  +publication-table-result-fields+)

(defun expand-select-targets (targets scope)
  "Expand records before analysis, as C ExpandRowReference does with FieldSelect."
  (loop for target in targets for raw = (getf target :expr)
        append
        (cond
          ((getf target :star)
           (cond ((typep scope 'select-scope)
                  (mapcar #'select-column-target (select-scope-columns scope)))
                 (scope
                  (mapcar #'select-column-target
                          (select-relation-columns
                           (make-select-relation :table scope :width (length (pg-class-columns scope))))))
                 (t (ereport "42601" "SELECT * requires a FROM clause"))))
          ((eq (first raw) :record-expand)
           (loop for (name type) in (sql-composite-call-fields (second raw))
                 collect (list :expr (list :record-field (copy-tree (second raw)) name)
                               :alias name)))
          (t (list target)))))

(defun analyzed-expression-contains-set-p (form)
  (and (consp form)
       (or (eq (first form) :set-func)
           (analyzed-expression-contains-set-p (car form))
           (analyzed-expression-contains-set-p (cdr form)))))

(defun analyze-publication-table-call (name arguments variadic)
  (unless *select-set-functions-allowed-p*
    (ereport "0A000" "set-returning functions are not allowed in this context"))
  (when (and (boundp '*inside-aggregate-input-p*) *inside-aggregate-input-p*)
    (ereport "0A000" "aggregate function calls cannot contain set-returning functions"))
  (when (some #'analyzed-expression-contains-set-p arguments)
    (ereport "0A000" "nested set-returning functions are not supported"))
  (let* ((filtered
           (and (not variadic) (= 2 (length arguments))
                (or (eq (expr-type (first arguments)) :text-array)
                    (and (in-unknown-constant-p (first arguments))
                         (eq (second (first arguments)) +null+)
                         (member (expr-type (second arguments)) '(:oid :int2 :int4 :int8))))))
         (array
           (cond
             (filtered (if (eq (expr-type (first arguments)) :text-array)
                           (first arguments)
                           (list :const +null+ :type :text-array :typmod -1)))
             (variadic
              (unless (= 1 (length arguments))
                (ereport "42883" "function ~a(~a) does not exist" name (call-signature arguments)))
              (let ((argument (first arguments)))
                (cond ((eq (expr-type argument) :text-array) argument)
                      ((and (in-unknown-constant-p argument) (eq (second argument) +null+))
                       (list :const +null+ :type :text-array :typmod -1))
                      (t (ereport "42883" "function ~a(~a) does not exist" name (call-signature arguments))))))
             (t
              (unless arguments (ereport "42883" "function ~a() does not exist" name))
              (unless (every (lambda (argument) (member (expr-type argument) '(:text :varchar :name)))
                             arguments)
                (ereport "42883" "function ~a(~a) does not exist" name (call-signature arguments)))
              (list :array arguments :type :text-array :typmod -1)))))
    (list :set-func name
          (if filtered (list array (coerce-catalog-oid-argument (second arguments) name))
              (list array))
          :type :record :typmod -1 :fields +publication-table-result-fields+)))

(defun analyze-record-field (raw scope)
  (let* ((record (analyze-expr (second raw) scope))
         (fields (getf (cdddr record) :fields))
         (position (position (third raw) fields :key #'first :test #'string=)))
    (unless (eq (expr-type record) :record)
      (ereport "42809" "expression is not a composite type"))
    (unless position (ereport "42703" "record has no field \"~a\"" (third raw)))
    (list :record-field record position :type (second (nth position fields)) :typmod -1)))

(defun analyze-expanded-target (target scope)
  "Keep already analyzed star columns, including merged JOIN USING expressions."
  (if (getf target :type) (copy-list target) (analyze-target target scope)))

(defun analyze-targets (targets table)
  (mapcar (lambda (target) (analyze-expanded-target target table))
          (expand-select-targets targets table)))

(defun prepare-select-project-set (targets order input-width distinct-p)
  "Match C's intermediate ProjectSet deduplication when SRFs need projection."
  (let* ((calls (make-array 0 :adjustable t :fill-pointer 0))
         (share-calls-p
           (some (lambda (expression)
                   (and (not (eq (first expression) :set-func))
                        (analyzed-expression-contains-set-p expression)))
                 (append (mapcar (lambda (target) (getf target :expr)) targets)
                         (unless distinct-p (mapcar #'car order))))))
    (labels ((lift (form reuse-p)
               (cond
                 ((atom form) form)
                 ((eq (first form) :set-func)
                  (let ((position
                          (and (or reuse-p share-calls-p)
                               (or (position form calls :test #'eq)
                                   (position form calls :test #'equal)))))
                    (unless position
                      (setf position (length calls))
                      (vector-push-extend form calls))
                    (list :col (+ input-width position 1) :type (expr-type form)
                          :typmod -1 :table-oid 0 :attnum 0)))
                 (t (cons (lift (car form) reuse-p) (lift (cdr form) reuse-p))))))
      (let* ((result-targets
               (mapcar (lambda (target)
                         (let ((copy (copy-list target)))
                           (setf (getf copy :expr) (lift (getf copy :expr) nil))
                           copy))
                       targets))
             (result-order
               (if distinct-p order
                   (mapcar
                    (lambda (item)
                      (let* ((position
                               (or (position (car item) targets
                                             :key (lambda (target) (getf target :expr)) :test #'eq)
                                   (position (car item) targets
                                             :key (lambda (target) (getf target :expr)) :test #'equal)))
                             (expression (if position
                                             (getf (nth position result-targets) :expr)
                                             (lift (car item) t))))
                        (cons expression (cdr item))))
                    order))))
        (values result-targets result-order (coerce calls 'list))))))

(defun sql-record-output (record)
  "Use C record_out's NULL fields, quoting, and doubled quote/backslash escapes."
  (with-output-to-string (stream)
    (write-char #\( stream)
    (loop for value across (sql-record-values record)
          for field in (sql-record-fields record)
          for position from 0
          do (when (plusp position) (write-char #\, stream))
             (unless (eq value +null+)
               (let* ((text (value->text value (second field)))
                      (quote-p (or (zerop (length text))
                                   (some (lambda (character)
                                           (or (find character "(),")
                                               (member (char-code character) '(9 10 11 12 13 32 34 92))))
                                         text))))
                 (when quote-p (write-char (code-char 34) stream))
                 (loop for character across text
                       do (when (member (char-code character) '(34 92))
                            (write-char character stream))
                          (write-char character stream))
                 (when quote-p (write-char (code-char 34) stream)))))
    (write-char #\) stream)))

(defun same-group-column-p (left right)
  "Compare resolved columns by identity, ignoring incidental nullability."
  (and (eq (first left) :col)
       (eq (first right) :col)
       (= (second left) (second right))
       (eq (expr-type left) (expr-type right))
       (= (expr-typmod left) (expr-typmod right))
       (= (or (getf left :table-oid) 0)
          (or (getf right :table-oid) 0))
       (= (or (getf left :attnum) 0)
          (or (getf right :attnum) 0))))

(defun group-column-position (expr group-keys)
  (position expr group-keys :test #'same-group-column-p))

(defun analyze-group-keys (group-asts scope)
  "Resolve a GROUP BY list and keep its first occurrence of each column."
  (let ((keys '()))
    (dolist (ast group-asts)
      (let ((expr (analyze-expr ast scope)))
        (unless (eq (first expr) :col)
          (ereport "0A000" "GROUP BY currently supports column references only"))
        (unless (group-column-position expr keys)
          (push expr keys))))
    (nreverse keys)))

(defun aggregate-output-ref (position type)
  "Reference the internal row produced by an aggregate plan."
  (list :col position :type type :typmod -1
        :table-oid 0 :attnum position))

(defun aggregate-row-independent-expression-p (expr)
  "True when an analyzed expression does not depend on an input row."
  (not (expression-contains-column-p expr)))

(defvar *select-aggregate-analysis* nil)

(defvar *aggregate-order-mode* nil)

(defun raw-aggregate-kind (form)
  (cond ((and (consp form) (eq (first form) :count-star)) :count)
        ((and (consp form) (eq (first form) :func)
              (string= (second form) "array_agg")
              (or (null (getf (cdddr form) :schema))
                  (string= (getf (cdddr form) :schema) "pg_catalog")))
         :array-agg)))

(defun contains-aggregate-p (form)
  (and (consp form)
       (or (raw-aggregate-kind form)
           (contains-aggregate-p (car form))
           (contains-aggregate-p (cdr form)))))

(defun analyzed-expression-volatile-p (form)
  "Only known stable/immutable builtins may share an aggregate transition."
  (and (consp form)
       (or (and (eq (first form) :func)
                (not (member (second form)
                             '("version" "pg_lisp_runtime" "current_database" "current_schema"
                               "current_catalog" "current_user" "session_user" "user"
                               "length" "char_length" "character_length" "pg_backend_pid")
                             :test #'string=)))
           (analyzed-expression-volatile-p (car form))
           (analyzed-expression-volatile-p (cdr form)))))

(defun analyze-aggregate-call (raw scope)
  (unless *select-aggregate-analysis*
    (ereport "42803" "aggregate functions are not allowed in this expression"))
  (when *inside-aggregate-input-p*
    (ereport "42803" "aggregate function calls cannot be nested"))
  (let* ((descriptors (getf *select-aggregate-analysis* :descriptors))
         (kind (raw-aggregate-kind raw))
         (type (if (eq kind :count) :int8 :text-array))
         (input
           (when (eq kind :array-agg)
             (unless (= 1 (length (third raw))) (ereport "42883" "array_agg requires one argument"))
             (let* ((*inside-aggregate-input-p* t)
                    (argument (analyze-expr (first (third raw)) scope)))
               (when (in-unknown-constant-p argument)
                 (ereport "42725" "function array_agg(unknown) is not unique"))
               (unless (eq (expr-type argument) :text)
                 (ereport "0A000" "array_agg currently supports text input"))
               argument)))
         (existing
           (when (or *aggregate-order-mode* (not (analyzed-expression-volatile-p input)))
             (position-if (lambda (descriptor)
                            (and (eq kind (getf descriptor :kind))
                                 (selected-expression-equal-p input (getf descriptor :input))))
                          descriptors)))
         (index (or existing (length descriptors))))
    (when (and (eq *aggregate-order-mode* :existing-only) (null existing))
      (ereport "42803" "aggregate is not present in the SELECT list"))
    (unless existing
      (vector-push-extend (list :kind kind :input input :type type :source-expr raw) descriptors))
    (append (aggregate-output-ref
             (+ (length (getf *select-aggregate-analysis* :group-keys)) index 1) type)
            (list :aggregate-output t))))

(defun aggregate-expression-signature (form descriptors)
  "Compare semantic aggregate identities independently of their result slots."
  (cond
    ((atom form) form)
    ((and (eq (first form) :col) (getf (cddr form) :aggregate-output))
     (let* ((offset (length (getf *select-aggregate-analysis* :group-keys)))
            (descriptor (aref descriptors (- (second form) offset 1))))
       (list :aggregate (getf descriptor :kind) (getf descriptor :input))))
    (t (cons (aggregate-expression-signature (car form) descriptors)
             (aggregate-expression-signature (cdr form) descriptors)))))

(defun aggregate-input-column-name (expression scope)
  (if (typep scope 'select-scope)
      (let ((column
              (find expression
                    (append (select-scope-columns scope)
                            (mapcan #'select-relation-columns (select-scope-relations scope)))
                    :key #'select-column-expr :test #'same-group-column-p)))
        (and column (select-column-name column)))
      (when scope
        (pg-column-name (aref (pg-class-columns scope) (1- (second expression)))))))

(defun rewrite-aggregate-columns (form group-keys &optional scope)
  "Rebase grouped input Vars while leaving lifted aggregate result slots intact."
  (cond
    ((atom form) form)
    ((eq (first form) :col)
     (if (getf (cddr form) :aggregate-output)
         form
         (let ((position (group-column-position form group-keys)))
           (unless position
             (ereport "42803"
                      "column \"~a\" must appear in the GROUP BY clause or be used in an aggregate function"
                      (or (aggregate-input-column-name form scope) "?column?")))
           (aggregate-output-ref (1+ position) (expr-type form)))))
    (t (cons (rewrite-aggregate-columns (car form) group-keys scope)
             (rewrite-aggregate-columns (cdr form) group-keys scope)))))

(defun analyze-aggregate-targets (raw-targets scope group-keys)
  "Lift aggregates recursively and validate each remaining Var against GROUP BY."
  (let* ((descriptors (make-array 0 :adjustable t :fill-pointer 0))
         (*select-aggregate-analysis* (list :group-keys group-keys :descriptors descriptors))
         (targets
           (loop for target in (expand-select-targets raw-targets scope)
                 for before = (length descriptors)
                 for analyzed = (analyze-expanded-target target scope)
                 do (setf (getf analyzed :expr)
                          (rewrite-aggregate-columns (getf analyzed :expr) group-keys scope))
                 when (contains-aggregate-p (getf target :expr))
                   do (setf (getf analyzed :source-expr)
                            (aggregate-expression-signature (getf analyzed :expr) descriptors)
                            (getf analyzed :aggregate-index) before
                            (getf analyzed :table-oid) 0 (getf analyzed :attnum) 0)
                 collect analyzed)))
    (values targets (coerce descriptors 'list) descriptors)))

(defun selected-order-target-position (item targets)
  "Resolve SQL92 ordinals and aliases; otherwise let the caller analyze expressions."
  (let* ((raw (getf item :expr))
         (aliases (when (and (eq (first raw) :column) (null (third raw)))
                    (remove-if-not (lambda (target) (string= (second raw) (getf target :name)))
                                   targets))))
    (when (and (eq (first raw) :const) (integerp (second raw)))
      (unless (<= 1 (second raw) (length targets))
        (ereport "42P10" "ORDER BY position is not in select list"))
      (return-from selected-order-target-position (values (1- (second raw)) t)))
    (when (member (first raw) '(:const :bool))
      (ereport "42601" "non-integer constant in ORDER BY"))
    (when aliases
      (unless (every (lambda (target) (selected-target-equal-p (first aliases) target))
                     (rest aliases))
        (ereport "42702" "ORDER BY \"~a\" is ambiguous" (second raw)))
      (return-from selected-order-target-position
        (values (position (first aliases) targets :test #'eq) t)))
    (values nil nil)))

(defun analyze-aggregate-order (order scope targets group-keys &optional descriptors allow-new)
  "Resolve output aliases first, then grouped expressions and selected aggregates."
  (let ((*select-set-functions-allowed-p* t)
        (*select-aggregate-analysis*
          (list :group-keys group-keys :descriptors (if (vectorp descriptors) descriptors (coerce descriptors 'vector))))
        (*aggregate-order-mode* (if allow-new :allow-new :existing-only)))
    (loop for item in order
          collect (cons
                   (multiple-value-bind (position selected-p)
                       (selected-order-target-position item targets)
                     (if selected-p
                         (getf (nth position targets) :expr)
                         (rewrite-aggregate-columns (analyze-expr (getf item :expr) scope)
                                                    group-keys scope)))
                   (getf item :desc)))))

(defun analyze-order (order table targets)
  (let ((*select-set-functions-allowed-p* t))
    (loop for item in order
          collect
          (cons (multiple-value-bind (position selected-p) (selected-order-target-position item targets)
                  (if selected-p (getf (nth position targets) :expr)
                      (analyze-expr (getf item :expr) table)))
                (getf item :desc)))))

(defun sql-type-category (type)
  (cond ((member type '(:int2 :int4 :int8 :oid)) :numeric)
        ((member type '(:text :varchar :name)) :string)
        ((member type '(:int2vector :text-array)) :array)
        ((member type '(:char :pg-node-tree)) :internal)
        (t type)))

(defun sql-implicit-coercion-p (source target)
  "The supported implicit pg_cast edges; explicit I/O casts are broader."
  (or (eq source target)
      (member (list source target)
              '((:int2 :int4) (:int2 :int8) (:int2 :oid) (:int4 :int8) (:int4 :oid) (:int8 :oid)
                (:name :text) (:text :name) (:text :varchar)
                (:varchar :text) (:varchar :name)
                (:char :text) (:pg-node-tree :text))
              :test #'equal)))

(defun union-common-type (left right &optional (context "UNION"))
  (let* ((left-type (unless (in-unknown-constant-p (getf left :expr)) (getf left :type)))
         (right-type (unless (in-unknown-constant-p (getf right :expr)) (getf right :type)))
         (type (or left-type right-type :text)))
    (when (and left-type right-type (not (eq left-type right-type)))
      (unless (eq (sql-type-category left-type) (sql-type-category right-type))
        (ereport "42804" "~a types ~a and ~a cannot be matched" context left-type right-type))
      (when (and (not (member type '(:text :oid :bool)))
                 (sql-implicit-coercion-p left-type right-type)
                 (not (sql-implicit-coercion-p right-type left-type)))
        (setf type right-type)))
    (dolist (source (list left-type right-type))
      (when (and source (not (sql-implicit-coercion-p source type)))
        (ereport "42846" "~a could not convert type ~a to ~a" context source type)))
    (values type
            (if (and (eq left-type type) (eq right-type type)
                     (= (getf left :typmod) (getf right :typmod)))
                (getf left :typmod) -1))))

(defun union-input-expression (target slot type typmod)
  "Coerce projected arm values; resolve unknown constants during analysis."
  (let ((expression (getf target :expr))
        (source (getf target :type)))
    (cond
      ((in-unknown-constant-p expression)
       (list :const (sql-cast-value (second expression) :text type -1)
             :type type :typmod -1 :explicit-cast t))
      ((eq source type) (list :col slot :type type :typmod typmod))
      (t (list :cast (list :col slot :type source :typmod (getf target :typmod))
               :type type :typmod -1)))))

(defun analyze-union-targets (left right)
  (unless (= (length left) (length right))
    (ereport "42601" "each UNION query must have the same number of columns"))
  (let ((targets nil) (left-input nil) (right-input nil))
    (loop for ltarget in left for rtarget in right for slot from 1
          do (multiple-value-bind (type typmod) (union-common-type ltarget rtarget)
               (push (list :name (getf ltarget :name) :type type :typmod typmod
                           :table-oid 0 :attnum 0
                           :expr (list :col slot :type type :typmod typmod))
                     targets)
               (push (union-input-expression ltarget slot type typmod) left-input)
               (push (union-input-expression rtarget slot type typmod) right-input)))
    (values (nreverse targets) (list (nreverse left-input) (nreverse right-input)))))

(defun analyze-union-order (order targets)
  (mapcar
   (lambda (item)
     (multiple-value-bind (position selected-p) (selected-order-target-position item targets)
       (unless selected-p
         (let ((raw (getf item :expr)))
           (when (eq (first raw) :column)
             (if (third raw)
                 (ereport "42P01" "missing FROM-clause entry for table ~a" (second raw))
                 (ereport "42703" "column ~a does not exist" (second raw))))
           (ereport "0A000" "only result column names can be used in UNION ORDER BY")))
       (cons (getf (nth position targets) :expr) (getf item :desc))))
   order))

(defun selected-expression-equal-p (left right)
  (or (equal left right) (same-group-column-p left right)))

(defun selected-target-equal-p (left right)
  "Keep semantic aggregate identity separate from internal output slot numbers."
  (selected-expression-equal-p
   (getf left :source-expr (getf left :expr))
   (getf right :source-expr (getf right :expr))))

(defun distinct-order-target-position (item scope targets aggregate-p group-keys &optional descriptors)
  "Require a selected output as C transformDistinctClause does."
  (multiple-value-bind (position selected-p) (selected-order-target-position item targets)
    (when selected-p (return-from distinct-order-target-position position)))
  (let* ((*select-set-functions-allowed-p* t)
         (resolved (if aggregate-p
                       (caar (analyze-aggregate-order (list item) scope targets group-keys descriptors t))
                       (analyze-expr (getf item :expr) scope)))
         (position (position resolved targets :key (lambda (target) (getf target :expr))
                                              :test #'selected-expression-equal-p)))
    (unless position
      (ereport "42P10" "for SELECT DISTINCT, ORDER BY expressions must appear in select list"))
    position))

(defun projected-target-ref (target position)
  "Reference one projected output slot with the target's SQL type metadata."
  (list :col position :type (getf target :type) :typmod (getf target :typmod)))

(defun analyze-distinct-order (order scope targets aggregate-p group-keys &optional descriptors)
  "Order distinct rows using projected slots, without reevaluating targets."
  (loop for item in order
        for position = (distinct-order-target-position item scope targets aggregate-p group-keys descriptors)
        for target = (nth position targets)
        collect (cons (projected-target-ref target (1+ position))
                      (getf item :desc))))

;;; ---- evaluation ----

(defun true-p (v)
  (and v (not (eq v +null+))))

(defun compare-integer-vectors (left right)
  "Compare integer vector contents lexically, then length (as C byteacmp)."
  (loop for a across left
        for b across right
        when (< a b) do (return-from compare-integer-vectors -1)
          when (> a b) do (return-from compare-integer-vectors 1))
  (cond ((< (length left) (length right)) -1)
        ((> (length left) (length right)) 1)
        (t 0)))

(defun compare-sql-array-values (left right)
  (loop for a across left for b across right
        for comparison = (cond ((and (eq a +null+) (eq b +null+)) 0)
                               ((eq a +null+) 1) ((eq b +null+) -1)
                               (t (compare-3way a b)))
        unless (zerop comparison) do (return-from compare-sql-array-values comparison))
  (cond ((< (length left) (length right)) -1)
        ((> (length left) (length right)) 1) (t 0)))

(defun compare-3way (a b)
  (cond
    ((and (member a '(t nil)) (member b '(t nil)))
     (cond ((eq a b) 0) (a 1) (t -1)))
    ((and (numberp a) (numberp b))
     (cond ((< a b) -1) ((> a b) 1) (t 0)))
    ((and (stringp a) (stringp b))
     (cond ((string< a b) -1) ((string> a b) 1) (t 0)))
    ((and (vectorp a) (vectorp b)
          (every #'integerp a) (every #'integerp b))
     (compare-integer-vectors a b))
    ((and (sql-record-p a) (sql-record-p b))
     (unless (equal (mapcar #'second (sql-record-fields a))
                    (mapcar #'second (sql-record-fields b)))
       (ereport "42804" "cannot compare dissimilar record types"))
     (compare-sql-array-values (sql-record-values a) (sql-record-values b)))
    ((and (vectorp a) (vectorp b)) (compare-sql-array-values a b))
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

(defun sql-in-value (value candidates negated-p)
  "Evaluate membership with SQL NULL semantics and a single left-hand value."
  (let ((result
          (loop with null-seen-p = nil
                for candidate in candidates
                for match = (sql-compare "=" value candidate)
                when (eq match t) return t
                  when (eq match +null+) do (setf null-seen-p t)
                    finally (return (if null-seen-p +null+ nil)))))
    (if negated-p (sql-3val-not result) result)))

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
    ((string= name "pg_get_expr")
     (sql-strict-let ((node (first args)) (relation (second args)))
       (ereport "0A000" "non-NULL pg_node_tree deparsing is not supported")))
    ((string= name "pg_get_replica_identity_index")
     (sql-strict-let ((oid (first args))) (sql-replica-identity-index oid)))
    ((string= name "version")
     (format nil "PostgreSQL ~a (pg-lisp 0.1)" *server-version*))
    ((string= name "pg_lisp_runtime")
     (format nil "~a ~a" (lisp-implementation-type)
             (lisp-implementation-version)))
    ;; xlogfuncs.c returns the current WAL write location as a pg_lsn value.
    ((string= name "pg_current_wal_lsn")
     *wal-end-lsn*)
    ((string= name "pg_lisp_cluster_identity")
     (catalog-cluster-identity *catalog*))
    ((string= name "nextval")
     (unless (and (= (length args) 1) (stringp (first args)))
       (ereport "42883" "nextval expects one sequence name string"))
     (sequence-nextval (first args)))
    ((string= name "currval")
     (unless (and (= (length args) 1) (stringp (first args)))
       (ereport "42883" "currval expects one sequence name string"))
     (sequence-currval (first args)))
    ((string= name "set_config")
     (set-config-value (first args) (second args) (third args)))
    ((string= name "setval")
     (unless (and (member (length args) '(2 3))
                  (stringp (first args))
                  (integerp (second args))
                  (or (= (length args) 2)
                      (member (third args) '(t nil))))
       (ereport "42883"
                "setval expects a sequence name, int8 value, and optional boolean"))
     (sequence-setval (first args) (second args)
                      (if (= (length args) 3) (third args) t)))
    ((string= name "pg_lisp_acquire_subscription")
     (logical-acquire-subscription-owner (first args)))
    ((string= name "pg_lisp_replica_identity_columns")
     (with-write-lock
       (let* ((table-name (first args))
              (table (and (stringp table-name) (lookup-table table-name))))
         (unless table
           (ereport "42P01" "relation \"~a\" does not exist" table-name))
         (format nil "~{~d~^,~}"
                 (sort (copy-list (logical-table-replica-identity table)) #'<)))))
    ((string= name "pg_lisp_publication_definition")
     (with-write-lock
       (let* ((publication (lookup-publication (first args)))
              (db (current-database)))
         (unless publication
           (ereport "42704" "publication \"~a\" does not exist" (first args)))
         (catalog-readable-data-string
          (publication-definition-data publication db)))))
    ((string= name "pg_lisp_table_definition")
     (with-write-lock
       (let ((table (lookup-table (first args))))
         (unless table
           (ereport "42P01" "table \"~a\" does not exist" (first args)))
         (catalog-table-definition-sql table))))
    ((string= name "pg_lisp_sequence_state")
     (with-write-lock
       (let ((sequence (lookup-sequence (first args))))
         (unless sequence
           (ereport "42P01" "sequence \"~a\" does not exist" (first args)))
         (catalog-readable-data-string
          (list :name (pg-sequence-name sequence)
                :oid (pg-sequence-oid sequence)
                :start (pg-sequence-start-value sequence)
                :increment (pg-sequence-increment sequence)
                :min (pg-sequence-min-value sequence)
                :max (pg-sequence-max-value sequence)
                :last (pg-sequence-last-value sequence)
                :called (pg-sequence-called-p sequence))))))
    ((string= name "pg_lisp_replication_bootstrap")
     (with-write-lock
       (catalog-readable-data-string
        (catalog-replication-bootstrap-data
         (current-database) *current-snapshot*))))
    ((string= name "current_database")
     (backend-database *backend*))
    ((string= name "current_schema") (current-schema-name))
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

(defun eval-in-expression (expr env)
  "Evaluate C-style array groups followed by short-circuit row comparisons."
  (let ((array-items (getf (nthcdr 4 expr) :array-items))
        (negated-p (fourth expr))
        (null-seen-p nil))
    (when array-items
      (let ((result (sql-in-value
                     (eval-expr (getf (nthcdr 4 expr) :array-left) env)
                     (mapcar (lambda (item) (eval-expr item env)) array-items) nil)))
        (when (eq result t) (return-from eval-in-expression (not negated-p)))
        (when (eq result +null+) (setf null-seen-p t))))
    (dolist (comparison (getf (nthcdr 4 expr) :comparisons))
      (let ((result (sql-compare "=" (eval-expr (first comparison) env)
                                 (eval-expr (second comparison) env))))
        (when (eq result t) (return-from eval-in-expression (not negated-p)))
        (when (eq result +null+) (setf null-seen-p t))))
    (let ((result (if null-seen-p +null+ nil)))
      (if negated-p (sql-3val-not result) result))))

(defun eval-case-expression (expression environment)
  (dolist (branch (second expression))
    (when (eq (eval-expr (first branch) environment) t)
      (return-from eval-case-expression (eval-expr (second branch) environment))))
  (eval-expr (third expression) environment))

(defun eval-any-expression (expression environment)
  (let ((left (eval-expr (third expression) environment))
        (array (eval-expr (fourth expression) environment))
        (null-seen nil))
    (when (eq array +null+) (return-from eval-any-expression +null+))
    (loop for element across array
          for value = (sql-cast-value element (getf (nthcdr 4 expression) :element-source)
                                      (getf (nthcdr 4 expression) :element-target) -1)
          for result = (sql-compare (second expression) left value)
          do (when (eq result t) (return-from eval-any-expression t))
             (when (eq result +null+) (setf null-seen t)))
    (if null-seen +null+ nil)))

(defun eval-array-length (expression environment)
  (sql-strict-let ((array (eval-expr (second expression) environment))
                   (dimension (eval-expr (third expression) environment)))
    (if (and (= dimension 1)
             (or (eq (expr-type (second expression)) :int2vector)
                 (plusp (length array))))
        (length array) +null+)))

(defun eval-expr (expr env)
  (case (first expr)
    (:const (second expr))
    (:case (eval-case-expression expr env))
    (:any (eval-any-expression expr env))
    (:array-length (eval-array-length expr env))
    (:cast (sql-cast-value (eval-expr (second expr) env)
                           (expr-type (second expr)) (expr-type expr) (expr-typmod expr)))
    (:bool (second expr))
    (:array (map 'vector (lambda (argument) (eval-expr argument env)) (second expr)))
    (:record-field
     (let ((record (eval-expr (second expr) env)))
       (if (eq record +null+) +null+ (aref (sql-record-values record) (third expr)))))
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
           (let ((value (sql-arith op l r)))
             (if (eq (expr-type expr) :int2)
                 (sql-cast-value value :int2 :int2 -1) value)))))
    (:unary
     (sql-strict-let ((value (eval-expr (third expr) env)))
       (cond
         ((string= (second expr) "-")
          (let ((result (- value)))
            (if (eq (expr-type expr) :int2)
                (sql-cast-value result :int2 :int2 -1) result)))
         (t (ereport "42883" "unsupported unary operator ~a"
                     (second expr))))))
    (:and (sql-3val-and (eval-expr (second expr) env)
                        (eval-expr (third expr) env)))
    (:or (sql-3val-or (eval-expr (second expr) env)
                      (eval-expr (third expr) env)))
    (:not (sql-3val-not (eval-expr (second expr) env)))
    (:in (eval-in-expression expr env))
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

(defun sql-text-array-output (values)
  "Format one-dimensional text arrays with C array_out's quoting rules."
  (with-output-to-string (stream)
    (write-char #\{ stream)
    (loop for value across values for position from 0
          do (when (plusp position) (write-char #\, stream))
             (if (eq value +null+)
                 (write-string "NULL" stream)
                 (let ((quote-p (or (zerop (length value)) (string-equal value "NULL")
                                    (some (lambda (character)
                                            (or (find character "{}," )
                                                (member (char-code character) '(9 10 11 12 13 32 34 92))))
                                          value))))
                   (when quote-p (write-char (code-char 34) stream))
                   (loop for character across value
                         do (when (member (char-code character) '(34 92))
                              (write-char (code-char 92) stream))
                            (write-char character stream))
                   (when quote-p (write-char (code-char 34) stream)))))
    (write-char #\} stream)))

(defun value->text (v type)
  (cond
    ((eq v +null+) nil)
    ((eq type :bool) (if v "t" "f"))
    ((eq type :pg-lsn) (wal-format-lsn v))
    ((eq type :record) (sql-record-output v))
    ((eq type :text-array) (sql-text-array-output v))
    ((eq type :char) (sql-char-output v))
    ((eq type :int2vector) (format nil "~{~d~^ ~}" (coerce v 'list)))
    ((stringp v) v)
    (t (princ-to-string v))))
