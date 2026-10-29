;;;; analyze.lisp -- name resolution and expression evaluation.

(in-package #:pg)

;; A relation supplies a qualifier and a slice of the input row at OFFSET
;; with WIDTH entries.  ORIGINS carries source metadata; LATERAL-FORBIDDEN-P
;; retains an entry for lookup so an invalid reference can be diagnosed.
(defstruct select-relation
  name
  alias
  table
  origins
  lateral-forbidden-p
  (offset 0)
  (width 0))

;; A visible column pairs its name and executable expression with type,
;; typmod, and source table/attribute metadata used for result descriptions.
;; Its expression need not be a simple attribute (e.g. JOIN USING output).
(defstruct select-column
  name
  expr
  type
  (typmod -1)
  (table-oid 0)
  (attnum 0))

;; RELATIONS provides qualified lookup while COLUMNS is the unqualified
;; visible output, including merged JOIN USING columns.  PARENT links lexical
;; scopes; WIDTH is the input row width.  ANALYSIS-FRAME is transient statement
;; analysis ownership and is cleared after the complete statement is analyzed.
(defstruct select-scope
  (parent *select-outer-scope*)
  (analysis-frame *select-analysis-frame*)
  relations
  columns
  (width 0))

(defun select-relation-qualifier (relation)
  (or (select-relation-alias relation)
      (sql-relation-base-name (select-relation-name relation))))

(defvar *dml-source-target-scope* nil)

(defun resolve-select-column (scope qualifier name)
  ;; Search one scope at a time; outer references retain their lexical depth.
  ;; A forbidden matching entry raises 42P10 before ambiguity, matching C's
  ;; per-entry LATERAL check instead of treating the target as invisible.
  (loop for current = scope then (select-scope-parent current)
        for depth from 0 while current
        for relation = (and qualifier
                            (let ((qualified
                                    (remove-if-not
                                     (lambda (relation)
                                       (let ((name (select-relation-qualifier relation)))
                                         (and name (string= qualifier name))))
                                     (select-scope-relations current))))
                              (when (and (first qualified) (select-relation-lateral-forbidden-p (first qualified)))
                                (ereport "42P10" "invalid reference to the target table in a FROM subquery"))
                              (when (rest qualified)
                                (ereport "42P09" "table reference \"~a\" is ambiguous" qualifier))
                              (first qualified)))
        for columns = (if qualifier
                          (and relation (select-relation-columns relation))
                          (select-scope-columns current))
        for matches = (remove-if-not
                       (lambda (column) (string= name (select-column-name column))) columns)
        do (when (if qualifier
                     (and relation (select-relation-lateral-forbidden-p relation))
                     (some (lambda (item)
                             (and (select-relation-lateral-forbidden-p item)
                                  (find name (select-relation-columns item)
                                        :key #'select-column-name :test #'equal)))
                           (select-scope-relations current)))
             (ereport "42P10" "invalid reference to the target table in a FROM subquery"))
           (when (rest matches)
             (ereport "42702" "column reference \"~a\" is ambiguous" name))
           (when matches
             (let ((expression (select-column-expr (first matches))))
               (return-from resolve-select-column
                 (if (zerop depth) expression
                     (list :outer-ref depth expression
                           :type (expr-type expression) :typmod (expr-typmod expression))))))
           (when (and (null qualifier)
                      (eq (getf (select-scope-analysis-frame current) :kind) :returning)
                      (find name (select-scope-relations current)
                            :key #'select-relation-qualifier :test #'equal))
             (ereport "0A000" "whole-row RETURNING requires named composite types"))
           (when relation
             (ereport "42703" "column \"~a\" does not exist" name)))
  (if qualifier
      (ereport "42P01" "missing FROM-clause entry for table \"~a\"" qualifier)
      (ereport "42703" "column \"~a\" does not exist" name)))

(defun clear-query-analysis-contexts (prepared)
  "Drop transient ownership frames after the entire statement has been analyzed."
  (let ((queries (make-hash-table :test #'eq))
        (scopes (make-hash-table :test #'eq)))
    (labels ((scope (current)
               (when (and current (not (gethash current scopes)))
                 (setf (gethash current scopes) t
                       (select-scope-analysis-frame current) nil)
                 (scope (select-scope-parent current))))
             (query (state)
               (unless (gethash state queries)
                 (setf (gethash state queries) t)
                 (scope (getf state :scope))
                 (dolist (definition (getf state :ctes))
                   (query (sql-cte-prepared definition)))
                 (dolist (descriptor (getf state :sublinks))
                   (query (sql-subplan-prepared descriptor)))
                 (dolist (relation (getf state :source-relations))
                   (when (getf relation :subquery-prepared)
                     (query (getf relation :subquery-prepared))))
                 (when (getf state :set-operation)
                   (query (getf state :left))
                   (query (getf state :right))))))
      (query prepared)))
  prepared)

(defvar *sql-prng-state* nil
  "Connection-local xoroshiro128** state, initialized on first use.")

(defun sql-prng-u64 (value)
  (logand value #xffffffffffffffff))

(defun sql-prng-rotl (value bits)
  (sql-prng-u64 (logior (ash value bits) (ash value (- bits 64)))))

(defun sql-prng-seed (seed)
  (let ((value (sql-prng-u64 seed)))
    (flet ((next ()
             (setf value (sql-prng-u64 (+ value #x9e3779b97f4a7c15)))
             (let ((v value))
               (setf v (sql-prng-u64 (* (logxor v (ash v -30)) #xbf58476d1ce4e5b9)))
               (setf v (sql-prng-u64 (* (logxor v (ash v -27)) #x94d049bb133111eb)))
               (logxor v (ash v -31)))))
      (setf *sql-prng-state* (vector (next) (next))))))

(defun sql-prng-next ()
  (unless *sql-prng-state*
    (sql-prng-seed (random (ash 1 64) (make-random-state t))))
  (let* ((s0 (aref *sql-prng-state* 0))
         (sx (logxor (aref *sql-prng-state* 1) s0))
         (value (sql-prng-u64 (* (sql-prng-rotl (sql-prng-u64 (* s0 5)) 7) 9))))
    (setf (aref *sql-prng-state* 0)
          (sql-prng-u64 (logxor (sql-prng-rotl s0 24) sx (ash sx 16)))
          (aref *sql-prng-state* 1) (sql-prng-rotl sx 37))
    value))

(defun sql-prng-range (minimum maximum)
  (if (<= maximum minimum)
      minimum
      (let* ((range (- maximum minimum))
             (shift (- 64 (integer-length range))))
        (+ minimum (loop for value = (ash (sql-prng-next) (- shift))
                         when (<= value range) return value)))))

(defmacro sql-strict-let (bindings &body body)
  "Evaluate all BINDINGS once, returning SQL NULL if any value is NULL."
  `(let* ,bindings
     (if (or ,@(mapcar (lambda (binding)
                         `(eq ,(first binding) +null+))
                       bindings))
         +null+
         (progn ,@body))))

(defparameter +sql-expression-types+
  '((:jsonb 3802 -1) (:json 114 -1) (:bytea 17 -1) (:bool 16 1) (:timestamptz 1184 8) (:char 18 1) (:name 19 64) (:int8 20 8)
    (:int2 21 2) (:int2vector 22 -1) (:int4 23 4) (:text 25 -1) (:varchar 1043 -1)
    (:bytea-array 1001 -1) (:json-array 199 -1) (:jsonb-array 3807 -1)
    (:text-array 1009 -1) (:int2-array 1005 -1) (:int2vector-array 1006 -1) (:int4-array 1007 -1) (:int8-array 1016 -1)
    (:numeric-array 1231 -1) (:float4-array 1021 -1) (:float8-array 1022 -1)
    (:bool-array 1000 -1) (:char-array 1002 -1) (:name-array 1003 -1)
    (:varchar-array 1015 -1) (:oid-array 1028 -1) (:date-array 1182 -1)
    (:time-array 1183 -1) (:timetz-array 1270 -1) (:timestamp-array 1115 -1)
    (:timestamptz-array 1185 -1) (:interval-array 1187 -1) (:pg-lsn-array 3221 -1)
    (:bit-array 1561 -1) (:oid 26 4) (:xid 28 4) (:pg-node-tree 194 -1) (:record 2249 -1)
    (:void 2278 4) (:pg-lsn 3220 8) (:numeric 1700 -1) (:float4 700 4)
    (:float8 701 8) (:date 1082 4) (:time 1083 8) (:timetz 1266 12)
    (:timestamp 1114 8) (:interval 1186 16) (:bit 1560 -1)))

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
  (when (sql-array-element-type (expr-type expr))
    (return-from typed-arg-name
      (concatenate 'string (typed-arg-name (list :col 0 :type (sql-array-element-type (expr-type expr)))) "[]")))
  (case (expr-type expr)
    (:int2 "smallint")
    (:int4 "integer")
    (:int8 "bigint")
    (:pg-lsn "pg_lsn")
    (:oid "oid")
    (:void "void")
    (:record "record")
    (:pg-node-tree "pg_node_tree")
    (:date "date")
    (:time "time without time zone")
    (:timetz "time with time zone")
    (:timestamp "timestamp without time zone")
    (:interval "interval")
    (:timestamptz "timestamp with time zone")
    (:numeric "numeric")
    (:float4 "real")
    (:float8 "double precision")
    (:bit "bit")
    (:varchar "character varying")
    (:bool "boolean")
    (:text-array "text[]")
    (:int2-array "smallint[]") (:int4-array "integer[]") (:int8-array "bigint[]")
    (:numeric-array "numeric[]") (:float4-array "real[]") (:float8-array "double precision[]")
    (:int2vector "int2vector")
    (:char "\"char\"")
    (:name "name")
    (:json "json")
    (:jsonb "jsonb")
    (:bytea "bytea")
    (t "text")))

(defun call-signature (args)
  "Describe argument types before coercion, retaining unknown literals."
  (format nil "~{~a~^, ~}"
          (mapcar (lambda (argument)
                    (if (in-unknown-constant-p argument)
                        "unknown" (typed-arg-name argument)))
                  args)))

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

(defvar *defer-function-constant-coercion-p* nil
  "Defer input conversion while checking decorated scalar calls.")

(defun coerce-catalog-oid-argument (argument name)
  (unless (or (in-unknown-constant-p argument)
              (member (expr-type argument) '(:int2 :int4 :int8 :oid)))
    (ereport "42883" "function ~a(~a) does not exist" name (typed-arg-name argument)))
  (coerce-numeric-expression-to-oid (coerce-in-constant argument :oid)))

(defun analyze-metadata-function (name arguments)
  (cond
    ((string= name "pg_postmaster_start_time")
     (when arguments
       (ereport "42883" "function ~a(~a) does not exist" name (call-signature arguments)))
     (list :func name nil :type :timestamptz :typmod -1))
    ((string= name "array_length")
     (unless (and (= 2 (length arguments))
                  (or (eq (expr-type (first arguments)) :int2vector)
                      (sql-array-element-type (expr-type (first arguments)))))
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

(defun analyze-setting-function (name args)
  (unless (and (if (string= name "lower") (= (length args) 1)
                   (<= 1 (length args) 2))
               (member (expr-type (first args)) '(:text :varchar :name :char))
               (or (= (length args) 1)
                   (eq (expr-type (second args)) :bool)
                   (in-unknown-constant-p (second args))))
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
  (when (and (rest args) (in-unknown-constant-p (second args)))
    (setf (second args) (list :cast (second args) :type :bool :typmod -1)))
  (unless (eq (expr-type (first args)) :text)
    (setf (first args) (list :cast (first args) :type :text :typmod -1)))
  (list :func name args :type :text :typmod -1))

(defun sql-lower-c (value)
  "Match formatting.c:asc_tolower for the supported C/POSIX collation."
  (map 'string (lambda (character)
                 (let ((code (char-code character)))
                   (if (<= 65 code 90) (code-char (+ code 32)) character)))
       value))

(defun eval-setting-function (name args)
  (if (member +null+ args :test #'eq)
      +null+
      (if (string= name "lower")
          (sql-lower-c (first args))
          (sql-setting-value (first args) (second args)))))

(defun analyze-wal-lsn-diff (name arguments)
  "Resolve xlogfuncs.c's strict (pg_lsn, pg_lsn) -> numeric function."
  (unless (and (= (length arguments) 2)
               (every (lambda (argument)
                        (or (eq (expr-type argument) :pg-lsn)
                            (in-unknown-constant-p argument)))
                      arguments))
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature arguments)))
  (list :func name
        (mapcar (lambda (argument) (coerce-in-constant argument :pg-lsn)) arguments)
        :type :numeric :typmod -1))

(defun eval-wal-lsn-diff (arguments)
  "Match pg_lsn_mi without signed-int64 truncation or floating point."
  (sql-strict-let ((left (first arguments)) (right (second arguments)))
    (- left right)))

(defun analyze-wal-filename (name arguments)
  "Resolve xlogfuncs.c's strict pg_walfile_name(pg_lsn) -> text."
  (unless (and (= (length arguments) 1)
               (or (eq (expr-type (first arguments)) :pg-lsn)
                   (in-unknown-constant-p (first arguments))))
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature arguments)))
  (list :func name (list (coerce-in-constant (first arguments) :pg-lsn))
        :type :text :typmod -1))

(defun eval-wal-filename (arguments)
  "Compute XLogFileName; native WAL storage is not C disk compatible."
  (sql-strict-let ((location (first arguments)))
    (when (recovery-in-progress-p)
      (ereport "55000" "recovery is in progress"
               :hint "pg_walfile_name() cannot be executed during recovery."))
    (subseq (wal-segment-filename (floor location +wal-segment-size+)
                                  *wal-insert-timeline* :timeline) 0 24)))

(defun cast-expression-to-text (argument)
  (if (eq (expr-type argument) :text) argument
      (list :cast argument :type :text :typmod -1)))

(defun analyze-json-inspection-function (name arguments)
  "Resolve the four JSON inspection functions against their catalog signatures.
Unknown constants are coerced to the declared JSON or JSONB input type.  The
result is text for typeof and int4 for array_length."
  (let ((input-type (if (search "jsonb_" name) :jsonb :json)))
    (unless (and (= (length arguments) 1)
                 (or (eq (expr-type (first arguments)) input-type)
                     (in-unknown-constant-p (first arguments))))
      (ereport "42883" "function ~a(~a) does not exist" name (call-signature arguments)))
    (list :func name (list (coerce-common-expression (first arguments) input-type))
          :type (if (search "_typeof" name) :text :int4) :typmod -1)))

(defun analyze-jsonb-set-function (name arguments)
  "Resolve jsonb_set(jsonb, text[], jsonb [, boolean])."
  (let ((types '(:jsonb :text-array :jsonb :bool)))
    (unless (and (member (length arguments) '(3 4))
                 (loop for argument in arguments for type in types
                       always (or (eq (expr-type argument) type)
                                  (in-unknown-constant-p argument)
                                  (sql-implicit-coercion-p (expr-type argument) type))))
      (ereport "42883" "function ~a(~a) does not exist" name (call-signature arguments)))
    (list :func name
          (loop for argument in arguments for type in types
                collect (coerce-common-expression argument type))
          :type :jsonb :typmod -1)))

(defun analyze-to-jsonb-function (name arguments)
  "Resolve to_jsonb's polymorphic argument within supported scalar and array types."
  (unless (= (length arguments) 1)
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature arguments)))
  (let* ((argument (first arguments))
         (type (expr-type argument))
         (element-type (or (sql-array-element-type type) type)))
    (when (and (in-unknown-constant-p argument) (not *defer-function-constant-coercion-p*))
      (ereport "42804" "could not determine polymorphic type because input has type unknown"))
    (unless (or *defer-function-constant-coercion-p*
                (member element-type '(:text :varchar :name :bool :int2 :int4 :int8
                                       :numeric :float4 :float8 :json :jsonb :bytea :oid :char :bit :date
                                       :time :timetz :timestamp :timestamptz :interval :pg-lsn :int2vector)))
      (ereport "0A000" "to_jsonb for type ~a is not supported" type))
    (list :func name arguments :type :jsonb :typmod -1)))

(defun analyze-substr-function (name arguments)
  "Resolve text and bytea substring overloads."
  (unless (and (member (length arguments) '(2 3))
               (or (eq (expr-type (first arguments)) :bytea)
                   (sql-implicit-coercion-p (expr-type (first arguments)) :text))
               (every (lambda (argument)
                        (or (in-unknown-constant-p argument)
                            (member (expr-type argument) '(:int2 :int4))))
                      (rest arguments)))
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature arguments)))
  (let ((type (if (eq (expr-type (first arguments)) :bytea) :bytea :text)))
    (list :func name
          (cons (if (eq type :bytea) (first arguments)
                    (cast-expression-to-text (first arguments)))
                (mapcar (lambda (argument) (coerce-common-expression argument :int4))
                        (rest arguments)))
          :type type :typmod -1)))

(defun sql-substring-range (start length length-p)
  "Return a zero-based offset and a count, or NIL count for the tail."
  (when (and length-p (minusp length))
    (ereport "22011" "negative substring length not allowed"))
  (let ((adjusted (max 1 start)))
    (values (1- adjusted)
            (unless (or (not length-p) (> (+ start length) +int4-max+))
              (max 0 (- (+ start length) adjusted))))))

(defun eval-substr-function (arguments)
  "Match text and bytea substring position and length rules."
  (when (member +null+ arguments) (return-from eval-substr-function +null+))
  (destructuring-bind (value start &optional (length nil length-p)) arguments
    (multiple-value-bind (offset count) (sql-substring-range start length length-p)
      (let* ((size (length value))
             (begin (min size offset))
             (end (if count (min size (+ begin count)) size)))
        (subseq value begin end)))))

(defvar *toast-slice-current* nil)

(defun eval-toast-substring (arguments context type)
  "Evaluate a selected external bytea or text substring at its projection position."
  (let ((bounds (mapcar (lambda (argument) (eval-expr argument nil)) (rest arguments))))
    (when (member +null+ bounds)
      ;; Full text materialization validated UTF-8 even when a bound was NULL.
      (when (eq type :text)
        (destructuring-bind (toast pointer snapshot environment) context
          (declare (ignore environment))
          (unless toast (ereport "XX001" "external value has no owning TOAST relation"))
          (toast-fetch-text-slice toast pointer snapshot 0 0
                                  :index-reader #'select-toast-index-reader)))
      (return-from eval-toast-substring +null+))
    (when (and (eq type :text) (rest bounds) (minusp (second bounds)))
      ;; Preserve full-materialization error ordering for corrupt text.
      (destructuring-bind (toast pointer snapshot environment) context
        (declare (ignore environment))
        (unless toast (ereport "XX001" "external value has no owning TOAST relation"))
        (toast-fetch-text-slice toast pointer snapshot 0 0
                                :index-reader #'select-toast-index-reader)))
    (multiple-value-bind (offset count)
        (sql-substring-range (first bounds) (second bounds) (not (null (rest bounds))))
      (destructuring-bind (toast pointer snapshot environment) context
        (declare (ignore environment))
        (unless toast (ereport "XX001" "external value has no owning TOAST relation"))
        (if (eq type :bytea)
            (toast-fetch-value-slice toast pointer snapshot offset (or count #x40000000)
                                     :index-reader #'select-toast-index-reader)
            (toast-fetch-text-slice toast pointer snapshot offset count
                                    :index-reader #'select-toast-index-reader))))))

(defun eval-toast-length (expression context)
  "Evaluate a direct external length projection without materializing the value."
  (destructuring-bind (toast pointer snapshot environment) context
    (declare (ignore environment))
    (unless toast (ereport "XX001" "external value has no owning TOAST relation"))
    (let ((type (expr-type (first (third expression))))
          (name (second expression)))
      (if (or (eq type :bytea) (string= name "octet_length"))
          (toast-raw-value-size toast pointer snapshot)
          (toast-map-text-characters toast pointer snapshot nil
                                     :index-reader #'select-toast-index-reader)))))

(defun analyze-wal-receiver-function (name arguments)
  (when arguments
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature arguments)))
  (list :func name nil :type :record :typmod -1 :fields +wal-receiver-result-fields+))

(defun analyze-promotion-arguments (arguments table &optional variadic)
  "Bind pg_promote arguments in parameter order and supply defaults.
Check the signature before coercion, including named and VARIADIC arguments.
Convert supplied expressions in source order, then return them in parameter order."
  (let* ((analyzed
           (mapcar (lambda (argument)
                     (if (eq (first argument) :named-arg)
                         (list :named-arg (second argument) (analyze-expr (third argument) table))
                         (analyze-expr argument table)))
                   arguments))
         (names nil)
         (slots (vector (list :const t :type :bool :typmod -1)
                        (list :const 60 :type :int4 :typmod -1)))
         (assigned (vector nil nil))
         (position 0)
         (bindings nil))
    (dolist (argument analyzed)
      (if (eq (first argument) :named-arg)
          (progn
            (when (member (second argument) names :test #'equal)
              (ereport "42601" "argument name ~s used more than once" (second argument)))
            (push (second argument) names))
          (when names (ereport "42601" "positional argument cannot follow named argument"))))
    (dolist (argument analyzed)
      (let* ((named-p (eq (first argument) :named-arg))
             (index (if named-p (position (second argument) '("wait" "wait_seconds") :test #'equal)
                        (prog1 position (incf position))))
             (expression (if named-p (third argument) argument))
             (type (if (eql index 0) :bool :int4)))
        (unless (and index (< index 2) (not (aref assigned index))
                     (or (in-unknown-constant-p expression)
                         (if (eq type :bool) (eq (expr-type expression) :bool)
                             (member (expr-type expression) '(:int2 :int4)))))
          (ereport "42883" "function pg_promote(...) does not exist"))
        (push (list index expression type) bindings)
        (setf (aref assigned index) t)))
    (when (and variadic names (/= (caar bindings) (1- (length analyzed))))
      (ereport "42883" "function pg_promote(...) does not exist"))
    (dolist (binding (nreverse bindings))
      (destructuring-bind (index expression type) binding
        (setf (aref slots index) (coerce-common-expression expression type))))
    (coerce slots 'list)))

(defun random-call-signature (arguments &optional names)
  "Describe the caller's argument types before overload coercion.
Unknown literals retain PostgreSQL's unknown type in random/setseed errors;
NAMES keeps named notation in its original order, before min/max reordering."
  (format nil "~{~a~^, ~}"
          (loop for argument in arguments
                for index from 0
                for name = (nth index names)
                for type = (if (in-unknown-constant-p argument)
                               "unknown" (typed-arg-name argument))
                collect (if name (format nil "~a => ~a" name type) type))))

(defun analyze-random-arguments (arguments table &optional (name "random") schema)
  "Analyze random/setseed arguments in caller order, then bind random's min/max.
Return the original signature separately so resolution errors retain argument
names and unknown types.  setseed has no catalog argument names."
  (let ((analyzed (mapcar (lambda (argument)
                            (analyze-expr (if (eq (first argument) :named-arg)
                                              (third argument) argument) table))
                          arguments))
        (argument-names nil) (names nil)
        (slots (make-array 2 :initial-element nil))
        (position 0) (invalid nil) (overlapping nil))
    ;; transformFuncCall transforms every argument before ParseFuncOrColumn
    ;; checks named notation; an invalid expression takes precedence.
    (loop for argument in arguments
          for value in analyzed
          do (let* ((named (eq (first argument) :named-arg))
                    (arg-name (and named (second argument)))
                    (index (if named (position arg-name '("min" "max") :test #'equal)
                               (prog1 position (incf position)))))
               (when (and (not named) names)
                 (ereport "42601" "positional argument cannot follow named argument"))
               (when (and named (member arg-name names :test #'equal))
                 (ereport "42601" "argument name ~s used more than once" arg-name))
               (when (and named index (< index position)) (setf overlapping t))
               (when named (push arg-name names))
               (push arg-name argument-names)
               (if (and index (< index 2) (not (aref slots index)))
                   (setf (aref slots index) value)
                   (setf invalid t))))
    (let ((signature (random-call-signature analyzed (nreverse argument-names))))
      (when (and names
                 (or (string= name "setseed") invalid
                     (not (every #'identity slots))))
        (report-numeric-function-resolution-error
         name analyzed :schema schema :signature signature :named-p t :overlapping-p overlapping))
      (values (if names (coerce slots 'list) analyzed) signature))))

(defun numeric-function-p (name)
  "Recognize the supported mathematical catalog functions without argument names."
  (or (member name '("abs" "numeric_abs" "sign" "round" "trunc" "ceil" "ceiling" "floor"
                     "sqrt" "numeric_sqrt" "exp" "numeric_exp" "ln" "numeric_ln"
                     "log" "log10" "numeric_log" "power" "pow" "numeric_power" "dpow"
                     "scale" "min_scale" "trim_scale" "width_bucket") :test #'string=)
      (trig-function-p name)
      (float-catalog-alias name)
      (integer-catalog-alias-p name)
      (numeric-helper-function-p name)
      (numeric-arithmetic-function-p name)))

(defun numeric-function-argument-count-p (name count)
  "Match the supported numeric pg_proc overloads before checking argument names."
  (cond ((member name '("pi" "random") :test #'string=)
         (if (string= name "random") (member count '(0 2)) (zerop count)))
        ((string= name "width_bucket") (member count '(2 4)))
        ((member name '("round" "trunc" "log") :test #'string=) (member count '(1 2)))
        ((integer-catalog-alias-p name)
         (= count (if (member (subseq name 4) '("not" "abs") :test #'string=) 1 2)))
        ((numeric-helper-function-p name)
         (= count (if (member name '("numeric_inc" "numeric_uminus" "numeric_uplus")
                              :test #'string=) 1 2)))
        ((or (and (numeric-arithmetic-function-p name) (not (string= name "factorial")))
             (member name '("numeric_log" "power" "pow" "numeric_power" "dpow"
                            "atan2" "atan2d") :test #'string=))
         (= count 2))
        (t (= count 1))))

(defun report-numeric-function-resolution-error
    (name args &key schema signature ambiguous-p named-p overlapping-p)
  "Report supported numeric lookup failures using the caller's uncoerced types.
Like ParseFuncOrColumn, distinguish ambiguous candidates from missing functions;
for missing functions, count takes precedence over overlapping or unknown names,
then argument types. Only ambiguity and type failures carry the cast hint."
  (let ((wrong-count (not (numeric-function-argument-count-p name (length args)))))
    (ereport (if ambiguous-p "42725" "42883") "function ~a(~a) ~a"
             (if schema (format nil "~a.~a" schema name) name)
             (or signature (call-signature args))
             (if ambiguous-p "is not unique" "does not exist")
             :detail (cond (ambiguous-p "Could not choose a best candidate function.")
                           (wrong-count "No function of that name accepts the given number of arguments.")
                           (overlapping-p "In the closest available match, an argument was specified both positionally and by name.")
                           (named-p "No function of that name accepts the given argument names.")
                           (t "No function of that name accepts the given argument types."))
             :hint (when (or ambiguous-p (and (not wrong-count) (not named-p) (not overlapping-p)))
                     "You might need to add explicit type casts."))))

(defun analyze-numeric-call-arguments (name arguments table &optional schema)
  "Match ParseFuncOrColumn's errors for numeric calls using named notation."
  (let ((analyzed (mapcar (lambda (argument)
                            (analyze-expr (if (eq (first argument) :named-arg)
                                              (third argument) argument) table))
                          arguments))
        (names nil)
        (argument-names nil))
    ;; transformFuncCall analyzes all arguments before checking named notation.
    ;; These numeric pg_proc entries have no proargnames, so no named call binds.
    (dolist (argument arguments)
      (let ((arg-name (and (eq (first argument) :named-arg) (second argument))))
        (when (and (null arg-name) names)
          (ereport "42601" "positional argument cannot follow named argument"))
        (when (and arg-name (member arg-name names :test #'equal))
          (ereport "42601" "argument name \"~a\" used more than once" arg-name))
        (when arg-name (push arg-name names))
        (push arg-name argument-names)))
    ;; Lookup rejects these names regardless of types; arity determines which
    ;; ParseFuncOrColumn detail is reported, using the uncoerced signature.
    (report-numeric-function-resolution-error
     name analyzed :schema schema :named-p t
     :signature (random-call-signature analyzed (nreverse argument-names)))))

(defun physical-slot-function-signature (name)
  "Return the supported public slot function's input names, types and defaults."
  (cond
    ((string= name "pg_create_physical_replication_slot")
     '(("slot_name" :name :required) ("immediately_reserve" :bool nil) ("temporary" :bool nil)))
    ((string= name "pg_replication_slot_advance")
     '(("slot_name" :name :required) ("upto_lsn" :pg-lsn :required)))
    ((string= name "pg_drop_replication_slot") '((nil :name :required)))))

(defun analyze-catalog-call-inputs (arguments table)
  "Analyze catalog call arguments in caller order before binding their names."
  (let ((analyzed (mapcar (lambda (raw)
                            (analyze-expr (if (eq (first raw) :named-arg) (third raw) raw) table))
                          arguments))
        (names nil))
    (dolist (raw arguments)
      (let ((name (and (eq (first raw) :named-arg) (second raw))))
        (when (and (null name) names)
          (ereport "42601" "positional argument cannot follow named argument"))
        (when (and name (member name names :test #'equal))
          (ereport "42601" "argument name ~s used more than once" name))
        (when name (push name names))))
    (values analyzed names)))

(defun analyze-physical-slot-arguments (name arguments table variadic)
  "Bind supported physical slot arguments and defaults to the catalog signature."
  (multiple-value-bind (analyzed names) (analyze-catalog-call-inputs arguments table)
    (let* ((signature (physical-slot-function-signature name))
           (assigned (make-array (length signature) :initial-element nil))
           (slots (make-array (length signature) :initial-element nil))
           (position 0) (bindings nil))
      (loop for raw in arguments for expression in analyzed
            for named = (eq (first raw) :named-arg)
            for index = (if named (position (second raw) signature :key #'first :test #'equal)
                            (prog1 position (incf position)))
            for type = (and index (< index (length signature)) (second (nth index signature)))
            do (unless (and type (not (aref assigned index))
                            (or (in-unknown-constant-p expression)
                                (sql-implicit-coercion-p (expr-type expression) type)))
                 (ereport "42883" "function ~a with these arguments does not exist" name))
               (setf (aref assigned index) t)
               (push (list index expression type) bindings))
      (when (and variadic names (/= (caar bindings) (1- (length arguments))))
        (ereport "42883" "function ~a with these arguments does not exist" name))
      (loop for (parameter type default) in signature for index from 0
            unless (aref assigned index)
              do (when (eq default :required)
                   (ereport "42883" "function ~a(~a) does not exist" name (call-signature analyzed)))
                 (setf (aref slots index) (list :const default :type type :typmod -1)))
      (dolist (binding (nreverse bindings))
        (destructuring-bind (index expression type) binding
          (setf (aref slots index) (coerce-common-expression expression type))))
      (coerce slots 'list))))

(defun analyze-physical-slot-function (name arguments)
  "Resolve the result type of a supported public slot function."
  (if (string= name "pg_drop_replication_slot")
      (list :func name arguments :type :void :typmod -1)
      (list :func name arguments :type :record :typmod -1
                                 :fields (if (string= name "pg_create_physical_replication_slot")
                                             +physical-slot-create-result-fields+ +physical-slot-advance-result-fields+))))

(defun eval-physical-slot-function (name arguments)
  "Execute a bound public slot call using its evaluated SQL arguments."
  (if (some (lambda (argument) (eq argument +null+)) arguments)
      +null+
      (cond
        ((string= name "pg_create_physical_replication_slot")
         (let ((slot (wal-create-physical-replication-slot
                      (first arguments) :immediately-reserve (second arguments)
                      :temporary (third arguments))))
           (make-sql-record
            :fields +physical-slot-create-result-fields+
            :values (vector (wal-replication-slot-name slot)
                            (or (wal-replication-slot-restart-lsn slot) +null+)))))
        ((string= name "pg_replication_slot_advance")
         (make-sql-record
          :fields +physical-slot-advance-result-fields+
          :values (vector (first arguments)
                          (wal-advance-physical-replication-slot (first arguments) (second arguments)))))
        (t (wal-drop-replication-slot (first arguments)) nil))))

(defun analyze-call-argument-list (name arguments table &optional variadic schema)
  "Analyze call arguments, binding supported named arguments."
  (cond ((physical-slot-function-signature name)
         (analyze-physical-slot-arguments name arguments table variadic))
        ((string= name "pg_get_replication_slots")
         (analyze-catalog-call-inputs arguments table))
        ((string= name "pg_promote")
         (analyze-promotion-arguments arguments table variadic))
        ((member name '("random" "setseed") :test #'string=)
         (analyze-random-arguments arguments table name schema))
        ((and (numeric-function-p name)
              (some (lambda (argument) (eq (first argument) :named-arg)) arguments))
         (analyze-numeric-call-arguments name arguments table schema))
        (t (mapcar (lambda (argument)
                     (when (eq (first argument) :named-arg)
                       (ereport "0A000" "named arguments are not supported for function ~a" name))
                     (analyze-expr argument table)) arguments))))

(defun analyze-nullary-function (name args)
  "Check that a supported nullary function has no arguments and assign its result type."
  (when args
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
  (list :func name args
        :type (cond ((member name '("pg_current_wal_lsn" "pg_current_wal_flush_lsn"
                                    "pg_last_wal_receive_lsn" "pg_last_wal_replay_lsn")
                             :test #'string=) :pg-lsn)
                    ((member name '("pg_is_in_recovery" "pg_is_wal_replay_paused") :test #'string=) :bool)
                    ((member name '("pg_wal_replay_pause" "pg_wal_replay_resume") :test #'string=) :void)
                    ((string= name "pg_lisp_cluster_identity") :varchar)
                    (t :text))
        :typmod -1))

(defun analyze-trig-function (name args)
  "Resolve the float8-only mathematical functions."
  (unless (and (= (length args) (cond ((string= name "pi") 0)
                                      ((member name '("atan2" "atan2d") :test #'string=) 2)
                                      (t 1)))
               (every (lambda (arg)
                        (or (in-unknown-constant-p arg)
                            (sql-implicit-coercion-p (expr-type arg) :float8)))
                      args))
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
  (list :func name (mapcar (lambda (arg) (coerce-common-expression arg :float8)) args)
        :type :float8 :typmod -1))

(defun trig-function-p (name)
  (member name '("pi" "cbrt" "dcbrt" "degrees" "radians" "sin" "cos" "tan" "cot"
                 "asin" "acos" "atan" "atan2" "sind" "cosd" "tand" "cotd"
                 "asind" "acosd" "atand" "atan2d" "sinh" "cosh" "tanh"
                 "asinh" "acosh" "atanh") :test #'string=))

(defun analyze-basic-numeric-function (name args)
  "Resolve the supported basic numeric overload and coerce its arguments.
Two-argument round and trunc use numeric and an int4 scale.  Unary abs
preserves a known input type.  The numeric_ aliases choose numeric; other
unary calls choose numeric for numeric inputs and float8 otherwise.  An
unknown unary trunc argument is ambiguous because its catalog overloads
include different type categories."
  ;; trunc also has macaddr/macaddr8 overloads. An unknown unary argument
  ;; cannot choose a type category even though float8 is preferred numeric.
  (when (and (string= name "trunc") (= (length args) 1)
             (in-unknown-constant-p (first args)))
    (ereport "42725" "function ~a(~a) is not unique" name (call-signature args)))
  (unless (and (if (member name '("round" "trunc") :test #'string=)
                   (member (length args) '(1 2))
                   (= (length args) 1))
               (or (in-unknown-constant-p (first args))
                   (member (expr-type (first args)) '(:int2 :int4 :int8 :numeric :float4 :float8)))
               (or (= (length args) 1)
                   (in-unknown-constant-p (second args))
                   (member (expr-type (second args)) '(:int2 :int4))))
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
  (let* ((argument (first args))
         (type (cond ((or (= (length args) 2) (member name '("numeric_abs" "numeric_sqrt" "numeric_exp" "numeric_ln") :test #'string=)) :numeric)
                     ((and (string= name "abs")
                           (not (in-unknown-constant-p argument)))
                      (expr-type argument))
                     ((eq (expr-type argument) :numeric)
                      (if (in-unknown-constant-p argument) :float8 :numeric))
                     (t :float8))))
    (unless (or (in-unknown-constant-p argument)
                (sql-implicit-coercion-p (expr-type argument) type))
      (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
    (list :func name
          (cons (coerce-common-expression argument type)
                (mapcar (lambda (arg) (coerce-common-expression arg :int4))
                        (rest args)))
          :type type :typmod -1)))

(defun analyze-numeric-scale-function (name args)
  "Resolve numeric scale inspection and trimming functions."
  (unless (and (= (length args) 1)
               (or (in-unknown-constant-p (first args))
                   (sql-implicit-coercion-p (expr-type (first args)) :numeric)))
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
  (list :func name (list (coerce-common-expression (first args) :numeric))
        :type (if (string= name "trim_scale") :numeric :int4) :typmod -1))

(defun numeric-helper-function-p (name)
  (member name '("numeric_eq" "numeric_ne" "numeric_gt" "numeric_ge"
                 "numeric_lt" "numeric_le" "numeric_cmp" "numeric_inc"
                 "numeric_smaller" "numeric_larger" "numeric_uminus" "numeric_uplus")
          :test #'string=))

(defun analyze-numeric-helper-function (name args)
  (let ((arity (if (member name '("numeric_inc" "numeric_uminus" "numeric_uplus")
                           :test #'string=) 1 2)))
    (unless (and (= (length args) arity)
                 (every (lambda (arg)
                          (or (in-unknown-constant-p arg)
                              (sql-implicit-coercion-p (expr-type arg) :numeric))) args))
      (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
    (list :func name (mapcar (lambda (arg) (coerce-common-expression arg :numeric)) args)
          :type (cond ((member name '("numeric_eq" "numeric_ne" "numeric_gt" "numeric_ge"
                                      "numeric_lt" "numeric_le") :test #'string=) :bool)
                      ((string= name "numeric_cmp") :int4)
                      (t :numeric))
          :typmod -1)))

(defun eval-numeric-helper-function (name args)
  "Evaluate strict numeric helpers; equal extrema select the second argument.
Selecting the argument itself preserves its display scale."
  (when (member +null+ args) (return-from eval-numeric-helper-function +null+))
  (let ((left (first args)) (right (second args)))
    (cond ((string= name "numeric_inc") (sql-numeric-arithmetic "+" left 1))
          ((string= name "numeric_uminus") (sql-numeric-negate left))
          ((string= name "numeric_uplus") left)
          (t (let ((cmp (sql-numeric-compare left right)))
               (cond ((string= name "numeric_cmp") cmp)
                     ((string= name "numeric_eq") (zerop cmp))
                     ((string= name "numeric_ne") (not (zerop cmp)))
                     ((string= name "numeric_gt") (plusp cmp))
                     ((string= name "numeric_ge") (not (minusp cmp)))
                     ((string= name "numeric_lt") (minusp cmp))
                     ((string= name "numeric_le") (not (plusp cmp)))
                     ((string= name "numeric_smaller") (if (minusp cmp) left right))
                     (t (if (plusp cmp) left right))))))))

(defun numeric-arithmetic-function-p (name)
  "Recognize immutable numeric arithmetic function names."
  (member name '("div" "mod" "numeric_div_trunc" "numeric_mod"
                 "numeric_add" "numeric_sub" "numeric_mul" "numeric_div" "gcd" "lcm" "factorial")
          :test #'string=))

(defun analyze-numeric-arithmetic-function (name args)
  "Resolve two-argument arithmetic functions using the supported catalog overloads.
MOD, GCD and LCM select numeric or the widest known integer type; two unknown
arguments are ambiguous. GCD and LCM also reject two smallint arguments as
ambiguous. The other names resolve to numeric, subject to implicit coercion."
  (unless (= (length args) 2)
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
  (let* ((known (remove-if #'in-unknown-constant-p args))
         (type (cond ((not (member name '("mod" "gcd" "lcm") :test #'string=)) :numeric)
                     ((null known) (ereport "42725" "function ~a(~a) is not unique" name (call-signature args)))
                     ((some (lambda (arg) (eq (expr-type arg) :numeric)) known) :numeric)
                     ((some (lambda (arg) (eq (expr-type arg) :int8)) known) :int8)
                     ((some (lambda (arg) (eq (expr-type arg) :int4)) known) :int4)
                     ((and (not (string= name "mod"))
                           (every (lambda (arg) (eq (expr-type arg) :int2)) known))
                      (ereport "42725" "function ~a(~a) is not unique" name (call-signature args)))
                     (t :int2))))
    (unless (every (lambda (arg) (or (in-unknown-constant-p arg)
                                     (sql-implicit-coercion-p (expr-type arg) type))) args)
      (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
    (list :func name (mapcar (lambda (arg) (coerce-common-expression arg type)) args)
          :type type :typmod -1)))

(defun analyze-factorial-function (name args)
  "Resolve factorial's single bigint argument and numeric result."
  (unless (and (= (length args) 1)
               (or (in-unknown-constant-p (first args))
                   (sql-implicit-coercion-p (expr-type (first args)) :int8)))
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
  (list :func name (list (coerce-common-expression (first args) :int8))
        :type :numeric :typmod -1))

(defun analyze-log-function (name args)
  "Resolve one-argument or numeric two-argument logarithm overloads."
  (cond
    ((and (= (length args) 1) (not (string= name "numeric_log")))
     (analyze-basic-numeric-function name args))
    ((and (= (length args) 2) (not (string= name "log10"))
          (every (lambda (arg)
                   (or (in-unknown-constant-p arg)
                       (sql-implicit-coercion-p (expr-type arg) :numeric)))
                 args))
     (list :func name (mapcar (lambda (arg) (coerce-common-expression arg :numeric)) args)
           :type :numeric :typmod -1))
    (t (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))))

(defun resolve-power-operands (name args)
  "Resolve and coerce the two operands of a power function or operator.
A known numeric operand selects numeric unless a float operand is present;
otherwise the preferred float8 overload is used.  The catalog aliases dpow
and numeric_power select their declared types."
  (unless (= (length args) 2)
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
  (let* ((numeric (and (not (string= name "dpow"))
                       (or (string= name "numeric_power")
                           (and (some (lambda (arg) (and (not (in-unknown-constant-p arg))
                                                         (eq (expr-type arg) :numeric))) args)
                                (notany (lambda (arg) (member (expr-type arg) '(:float4 :float8))) args)))))
         (type (if numeric :numeric :float8)))
    (unless (every (lambda (arg) (or (in-unknown-constant-p arg)
                                     (sql-implicit-coercion-p (expr-type arg) type))) args)
      (ereport "42883" "function ~a(~a) does not exist" name (call-signature args)))
    (values (mapcar (lambda (arg) (coerce-common-expression arg type)) args) type)))

(defun analyze-power-function (name args)
  "Analyze a numeric or float8 power function call."
  (multiple-value-bind (arguments type) (resolve-power-operands name args)
    (list :func name arguments :type type :typmod -1)))

(defun analyze-width-bucket (args)
  ;; Resolve compatible scalar/array thresholds or numeric/float8 ranges to int4.
  (cond
    ((= (length args) 2)
     (let* ((operand (first args)) (array (second args))
                                   (element (if (eq (expr-type array) :int2vector) :int2
                                                (sql-array-element-type (expr-type array)))))
       (unless (or element (in-unknown-constant-p array))
         (ereport "42883" "function width_bucket(~a) does not exist" (call-signature args)))
       (when (and element (not (in-unknown-constant-p operand))
                  (not (eq (sql-type-category element) (sql-type-category (expr-type operand)))))
         (ereport "42883" "function width_bucket(~a) does not exist" (call-signature args)))
       ;; anycompatible considers the scalar before the array element.  Keep
       ;; this order: equally coercible types can retain the first candidate.
       (let* ((probe (and element (list :col 0 :type element :typmod -1)))
              (type (handler-case
                        (common-expression-type (if probe (list operand probe) (list operand)) "width_bucket")
                      (pg-error (error)
                        (if (member (pg-error-sqlstate error) '("42804" "42846") :test #'string=)
                            (ereport "42883" "function width_bucket(~a) does not exist" (call-signature args))
                            (error error)))))
              (array-type (sql-array-type type)))
         ;; Candidate compatibility is already checked; array type materialization
         ;; follows the aggregate decoration check in ParseFuncOrColumn.
         (when *defer-function-constant-coercion-p*
           (return-from analyze-width-bucket (list :func "width_bucket" args :type :int4 :typmod -1)))
         ;; Like get_array_type in polymorphic resolution, this lookup happens
         ;; before argument coercion or strict NULL evaluation. Ordinary array
         ;; types have no array type of their own; int2vector is exceptional
         ;; because pg_type also defines int2vector[] over it.
         (unless array-type
           (ereport "42704" "could not find array type for data type ~a"
                    (typed-arg-name (list :col 0 :type type))))
         (list :func "width_bucket"
               (list (coerce-common-expression operand type)
                     (if (eq (expr-type array) :int2vector)
                         (list :cast array :type array-type :typmod -1)
                         (coerce-common-expression array array-type)))
               :type :int4 :typmod -1))))
    ((= (length args) 4)
     (let* ((values (subseq args 0 3))
            (type (if (and (some (lambda (a) (and (not (in-unknown-constant-p a))
                                                  (eq (expr-type a) :numeric))) values)
                           (notany (lambda (a) (member (expr-type a) '(:float4 :float8))) values))
                      :numeric :float8)))
       (unless (and (every (lambda (a) (or (in-unknown-constant-p a)
                                           (sql-implicit-coercion-p (expr-type a) type))) values)
                    (or (in-unknown-constant-p (fourth args))
                        (sql-implicit-coercion-p (expr-type (fourth args)) :int4)))
         (ereport "42883" "function width_bucket(~a) does not exist" (call-signature args)))
       (list :func "width_bucket"
             (append (mapcar (lambda (a) (coerce-common-expression a type)) values)
                     (list (coerce-common-expression (fourth args) :int4)))
             :type :int4 :typmod -1)))
    (t (ereport "42883" "function width_bucket(~a) does not exist" (call-signature args)))))

(defun sql-width-bucket-timetz-bytes (elements)
  "Serialize non-NULL timetz elements in the tested C peer's little-endian 16-byte aligned layout.
The 12-byte payload contains an int64 time and int32 zone followed by padding."
  (let ((bytes (make-array (* 16 (length elements)) :element-type '(unsigned-byte 8) :initial-element 0)))
    (loop for value across elements for offset from 0 by 16 do
      (dotimes (i 8)
        (setf (aref bytes (+ offset i)) (ldb (byte 8 (* i 8)) (sql-timetz-microseconds value))))
      (dotimes (i 4)
        (setf (aref bytes (+ offset 8 i)) (ldb (byte 8 (* i 8)) (sql-timetz-zone-west value)))))
    bytes))

(defun sql-width-bucket-timetz-at (bytes index)
  "Decode a timetz at INDEX using C width_bucket_array_fixed's 12-byte typlen stride.
The tested peer stores aligned 16-byte elements, so later reads can span elements;
this models that platform-specific behavior rather than a portable C layout."
  (let ((offset (* 12 index)))
    (labels ((signed-word (start count)
               (let ((word (loop for i below count
                                 sum (ash (aref bytes (+ offset start i)) (* i 8)))))
                 (if (logbitp (1- (* count 8)) word) (- word (ash 1 (* count 8))) word))))
      (make-sql-timetz (signed-word 0 8) (signed-word 8 4)))))

(defun sql-width-bucket-timetz-compare (left right)
  "Compare the decoded fields using the tested C peer's wrapping int64 time arithmetic.
Zone-west breaks ties after comparing time plus zone-west in microseconds."
  (labels ((wrap64 (number)
             (let ((value (logand number #xffffffffffffffff)))
               (if (logbitp 63 value) (- value (ash 1 64)) value)))
           (key (value)
             (wrap64 (+ (sql-timetz-microseconds value)
                        (* (sql-timetz-zone-west value) 1000000)))))
    (let ((a (key left)) (b (key right)))
      (cond ((< a b) -1) ((> a b) 1)
            ((< (sql-timetz-zone-west left) (sql-timetz-zone-west right)) -1)
            ((> (sql-timetz-zone-west left) (sql-timetz-zone-west right)) 1)
            (t 0)))))

(defun sql-width-bucket-array (value array)
  ;; C width_bucket_array searches ascending thresholds for the first greater value.
  (when (> (length (sql-array-dimensions array)) 1)
    (ereport "2202E" "thresholds must be one-dimensional array"))
  (let* ((timetz-p (eq (sql-array-element-type-id array) :timetz))
         (elements (sql-array-elements array)))
    (when (find +null+ elements)
      (ereport "22004" "thresholds array must not contain NULLs"))
    (let ((left 0) (right (length elements))
                   (bytes (and timetz-p (sql-width-bucket-timetz-bytes elements))))
      (loop while (< left right)
            for mid = (floor (+ left right) 2)
            for comparison = (if timetz-p
                                 (sql-width-bucket-timetz-compare value (sql-width-bucket-timetz-at bytes mid))
                                 (compare-3way value (aref elements mid)))
            do (if (< comparison 0) (setf right mid) (setf left (1+ mid))))
      left)))

(defun sql-width-bucket-range (args)
  ;; C width_bucket_float8/width_bucket_numeric: equal-width ascending or
  ;; descending buckets, with exterior buckets 0 and COUNT+1.
  (destructuring-bind (operand bound1 bound2 count) args
    (when (<= count 0) (ereport "2201G" "count must be greater than zero"))
    (let* ((float-p (floatp bound1))
           (nan-p (lambda (v) (if float-p (sb-ext:float-nan-p v) (eq v :numeric-nan))))
           (infinite-p (lambda (v) (if float-p (sb-ext:float-infinity-p v)
                                       (member v '(:numeric-positive-infinity :numeric-negative-infinity)))))
           (compare (if float-p #'sql-float-compare #'sql-numeric-compare)))
      (when (or (funcall nan-p bound1) (funcall nan-p bound2))
        (ereport "2201G" "lower and upper bounds cannot be NaN"))
      (when (or (funcall infinite-p bound1) (funcall infinite-p bound2))
        (ereport "2201G" "lower and upper bounds must be finite"))
      (let ((direction (funcall compare bound1 bound2)))
        (when (zerop direction) (ereport "2201G" "lower bound cannot equal upper bound"))
        (let ((first-cmp (funcall compare operand bound1))
              (second-cmp (funcall compare operand bound2)))
          (cond ((if (minusp direction) (< first-cmp 0) (> first-cmp 0)) 0)
                ((if (minusp direction) (>= second-cmp 0) (<= second-cmp 0))
                 (when (= count +int4-max+) (ereport "22003" "integer out of range"))
                 (1+ count))
                (float-p
                 (let* ((a (if (minusp direction) operand bound1))
                        (b (if (minusp direction) bound1 operand))
                        (upper (if (minusp direction) bound2 bound1))
                        (lower (if (minusp direction) bound1 bound2))
                        (wide (> (- (rational upper) (rational lower)) (rational most-positive-double-float)))
                        (fraction (if wide
                                      (/ (- (/ a 2d0) (/ b 2d0)) (- (/ upper 2d0) (/ lower 2d0)))
                                      (/ (- a b) (- upper lower)))))
                   (1+ (min (1- count) (truncate (* count fraction))))))
                (t (1+ (floor (* count (- (sql-numeric-rational operand)
                                          (sql-numeric-rational bound1)))
                              (- (sql-numeric-rational bound2)
                                 (sql-numeric-rational bound1)))))))))))

(defun sql-width-bucket (args)
  ;; Both overloads are strict; dispatch already-coerced arguments by arity.
  (cond ((member +null+ args) +null+)
        ((= (length args) 2) (sql-width-bucket-array (first args) (second args)))
        (t (sql-width-bucket-range args))))

(defun analyze-random-function (name args &optional (signature (random-call-signature args)))
  (cond
    ((and (string= name "random") (null args))
     (list :func name args :type :float8 :typmod -1))
    ((and (string= name "setseed") (= (length args) 1)
          (or (in-unknown-constant-p (first args))
              (sql-implicit-coercion-p (expr-type (first args)) :float8)))
     (list :func name (list (coerce-common-expression (first args) :float8)) :type :void :typmod -1))
    ((and (string= name "random") (= (length args) 2))
     (when (every (lambda (arg) (or (in-unknown-constant-p arg)
                                    (eq (expr-type arg) :int2))) args)
       (ereport "42725" "function random(~a) is not unique" signature))
     (let* ((types (mapcar #'expr-type args))
            (type (cond ((member :numeric types) :numeric)
                        ((member :int8 types) :int8)
                        (t :int4))))
       (unless (every (lambda (arg) (or (in-unknown-constant-p arg)
                                        (sql-implicit-coercion-p (expr-type arg) type))) args)
         (ereport "42883" "function ~a(~a) does not exist" name signature))
       (list :func name (mapcar (lambda (arg) (coerce-common-expression arg type)) args)
             :type type :typmod -1)))
    (t (ereport "42883" "function ~a(~a) does not exist" name signature))))

(defun float-catalog-alias (name)
  "Return the declared argument type and execution name for a float catalog function.
Each listed function takes one argument and returns the same float type.  The
execution name selects a shared implementation; analysis keeps the catalog name
so expression identity remains distinct from the corresponding SQL function."
  (cdr (assoc name '(("float4abs" :float4 "abs")
                     ("float8abs" :float8 "abs")
                     ("dround" :float8 "round")
                     ("dtrunc" :float8 "trunc")
                     ("dsqrt" :float8 "sqrt")
                     ("dexp" :float8 "exp")
                     ("dlog1" :float8 "ln")
                     ("dlog10" :float8 "log10"))
              :test #'string=)))

(defun integer-catalog-alias-p (name)
  "Recognize supported integer catalog function names, retaining call identity.
The declared operand types are resolved during analysis; execution shares the
corresponding operator implementation without rewriting the analyzed call."
  (and (>= (length name) 5)
       (member (subseq name 0 4) '("int2" "int4" "int8") :test #'string=)
       (member (subseq name 4) '("and" "or" "xor" "not" "shl" "shr" "abs" "mod") :test #'string=)))

(defun analyze-integer-catalog-function (name args &optional schema)
  "Resolve a supported integer pg_proc helper without changing its call identity.
NAME must name a supported helper. Shift functions take int4 as their second
argument; other binary helpers take two operands of the named integer type.
Check the complete signature before coercing unknown literals, so resolution
errors precede input errors."
  (let* ((type (cdr (assoc (subseq name 0 4)
                           '(("int2" . :int2) ("int4" . :int4) ("int8" . :int8))
                           :test #'string=)))
         (suffix (subseq name 4))
         (unary (member suffix '("not" "abs") :test #'string=))
         (shift (member suffix '("shl" "shr") :test #'string=))
         (types (if unary (list type) (list type (if shift :int4 type)))))
    (unless (and (= (length args) (length types))
                 (every (lambda (arg target)
                          (or (in-unknown-constant-p arg)
                              (sql-implicit-coercion-p (expr-type arg) target)))
                        args types))
      (let ((wrong-count (/= (length args) (length types))))
        (ereport "42883" "function ~a(~a) does not exist"
                 (if schema (format nil "~a.~a" schema name) name)
                 (call-signature args)
                 :detail (if wrong-count
                             "No function of that name accepts the given number of arguments."
                             "No function of that name accepts the given argument types.")
                 :hint (unless wrong-count "You might need to add explicit type casts."))))
    (list :func name (mapcar #'coerce-common-expression args types)
          :type type :typmod -1)))

(defun analyze-numeric-function (name args schema &optional signature)
  "Resolve a numeric catalog call, retaining its original diagnostic signature.
Argument transformation and named-notation validation happen before this call,
so a nested argument error keeps its own diagnostics."
  (handler-case
      (cond
        ((integer-catalog-alias-p name) (analyze-integer-catalog-function name args schema))
        ((float-catalog-alias name)
         (let ((type (first (float-catalog-alias name))))
           (unless (and (= (length args) 1)
                        (or (in-unknown-constant-p (first args))
                            (sql-implicit-coercion-p (expr-type (first args)) type)))
             (report-numeric-function-resolution-error name args :schema schema))
           (list :func name (list (coerce-common-expression (first args) type))
                 :type type :typmod -1)))
        ((member name '("random" "setseed") :test #'string=)
         (analyze-random-function name args signature))
        ((trig-function-p name) (analyze-trig-function name args))
        ((string= name "width_bucket") (analyze-width-bucket args))
        ((member name '("abs" "numeric_abs" "sign" "round" "trunc" "ceil" "ceiling" "floor"
                        "sqrt" "numeric_sqrt" "exp" "numeric_exp" "ln" "numeric_ln")
                 :test #'string=)
         (analyze-basic-numeric-function name args))
        ((member name '("log" "log10" "numeric_log") :test #'string=)
         (analyze-log-function name args))
        ((member name '("power" "pow" "numeric_power" "dpow") :test #'string=)
         (analyze-power-function name args))
        ((member name '("scale" "min_scale" "trim_scale") :test #'string=)
         (analyze-numeric-scale-function name args))
        ((string= name "factorial") (analyze-factorial-function name args))
        ((numeric-helper-function-p name) (analyze-numeric-helper-function name args))
        ((numeric-arithmetic-function-p name) (analyze-numeric-arithmetic-function name args)))
    (pg-error (condition)
      (if (member (pg-error-sqlstate condition) '("42883" "42725") :test #'string=)
          (report-numeric-function-resolution-error
           name args :schema schema :signature signature
                     :ambiguous-p (string= (pg-error-sqlstate condition) "42725"))
          (error condition)))))

(defun report-qualified-numeric-call-error (name schema arguments table order)
  "Report lookup failure for a supported numeric name outside pg_catalog."
  ;; C transformFuncCall transforms every argument before ParseFuncOrColumn
  ;; checks named notation; FuncnameGetCandidates then resolves the explicit
  ;; schema.  Preserve that order even when the schema cannot contain the
  ;; supported builtin, and retain the uncoerced types for the signature.
  (let ((args (mapcar (lambda (argument)
                        (analyze-expr (if (eq (first argument) :named-arg)
                                          (third argument) argument) table))
                      arguments))
        (names nil)
        (argument-names nil))
    (dolist (argument arguments)
      (let ((arg-name (and (eq (first argument) :named-arg) (second argument))))
        (when (and (null arg-name) names)
          (ereport "42601" "positional argument cannot follow named argument"))
        (when (and arg-name (member arg-name names :test #'equal))
          (ereport "42601" "argument name \"~a\" used more than once" arg-name))
        (when arg-name (push arg-name names))
        (push arg-name argument-names)))
    (unless (member schema '("public" "pg_catalog") :test #'string=)
      (ereport "3F000" "schema \"~a\" does not exist" schema))
    ;; ParseFuncOrColumn adds this aggregate hint only for multiple sort keys.
    (ereport "42883" "function ~a.~a(~a) does not exist"
             schema name (random-call-signature args (nreverse argument-names))
             :detail (when (> (length order) 1)
                       "No aggregate function matches the given name and argument types.")
             :hint (when (> (length order) 1)
                     "Perhaps you misplaced ORDER BY; ORDER BY must appear after all regular arguments of the aggregate."))))

(defun logical-helper-function-p (name)
  (member name '("pg_lisp_acquire_subscription" "pg_lisp_replica_identity_columns"
                 "pg_lisp_publication_definition" "pg_lisp_table_definition"
                 "pg_lisp_sequence_state" "pg_lisp_replication_bootstrap")
          :test #'string=))

(defun analyze-logical-helper-function (name args)
  (cond
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
    ))

(defun check-qualified-numeric-function-call (expr table)
  (let ((schema (getf (cdddr expr) :schema))
        (name (second expr)))
    (when (and schema (not (string= schema "pg_catalog"))
               (or (numeric-function-p name)
                   (member name '("random" "setseed") :test #'string=)))
      (report-qualified-numeric-call-error name schema (third expr) table
                                           (getf (cdddr expr) :call-order)))))

(defun analyze-function-expression (expr table)
  "Resolve a supported scalar or administrative function call."
  (check-qualified-numeric-function-call expr table)
  (let* ((name (builtin-function-name (second expr) (getf (cdddr expr) :schema)))
         (random-signature nil)
         (args (multiple-value-bind (arguments signature)
                   (analyze-call-argument-list name (third expr) table (getf (cdddr expr) :variadic)
                                               (getf (cdddr expr) :schema))
                 (setf random-signature signature)
                 arguments)))
    (when (member name '("row_number" "rank" "dense_rank") :test #'string=)
      (when args (ereport "42883" "function ~a with these arguments does not exist" name))
      (ereport "42809" "window function ~a requires an OVER clause" name))
    ;; C ParseFuncOrColumn clears explicit VARIADIC for a nonvariadic match.
    (when (string= name "pg_get_publication_tables")
      (return-from analyze-function-expression
        (analyze-publication-table-call name args (getf (cdddr expr) :variadic))))
    (cond
      ((string= name "pg_get_replication_slots") (analyze-replication-slot-call name args))
      ((physical-slot-function-signature name) (analyze-physical-slot-function name args))
      ((or (numeric-function-p name)
           (member name '("random" "setseed") :test #'string=))
       ;; ParseFuncOrColumn selects the candidate before rejecting decorations,
       ;; then coerces arguments. Defer constant input conversion until that check.
       (let ((*defer-function-constant-coercion-p*
               (or (getf (cdddr expr) :call-distinct)
                   (getf (cdddr expr) :call-order))))
         (analyze-numeric-function name args (getf (cdddr expr) :schema) random-signature)))
      ((string= name "to_jsonb")
       (let ((*defer-function-constant-coercion-p*
               (or (getf (cdddr expr) :call-distinct)
                   (getf (cdddr expr) :call-order))))
         (analyze-to-jsonb-function name args)))
      ((member name '("jsonb_set" "jsonb_insert") :test #'string=)
       (let ((*defer-function-constant-coercion-p*
               (or (getf (cdddr expr) :call-distinct)
                   (getf (cdddr expr) :call-order))))
         (analyze-jsonb-set-function name args)))
      ((string= name "pg_promote") (list :func name args :type :bool :typmod -1))
      ((string= name "pg_stat_get_wal_receiver") (analyze-wal-receiver-function name args))
      ((member name '("json_typeof" "jsonb_typeof" "json_array_length" "jsonb_array_length")
               :test #'string=)
       (let ((*defer-function-constant-coercion-p*
               (or (getf (cdddr expr) :call-distinct)
                   (getf (cdddr expr) :call-order))))
         (analyze-json-inspection-function name args)))
      ((member name '("substr" "substring") :test #'string=)
       (let ((*defer-function-constant-coercion-p*
               (or (getf (cdddr expr) :call-distinct)
                   (getf (cdddr expr) :call-order))))
         (analyze-substr-function name args)))
      ((string= name "pg_wal_lsn_diff") (analyze-wal-lsn-diff name args))
      ((string= name "pg_walfile_name") (analyze-wal-filename name args))
      ((member name '("lower" "current_setting") :test #'string=)
       (analyze-setting-function name args))
      ((member name '("array_length" "pg_get_expr" "pg_get_replica_identity_index"
                      "pg_postmaster_start_time") :test #'string=)
       (analyze-metadata-function name args))
      ((member name '("version" "pg_lisp_runtime" "pg_current_wal_lsn"
                      "pg_current_wal_flush_lsn" "pg_is_in_recovery"
                      "pg_last_wal_receive_lsn" "pg_last_wal_replay_lsn"
                      "pg_wal_replay_pause" "pg_wal_replay_resume"
                      "pg_is_wal_replay_paused" "pg_get_wal_replay_pause_state"
                      "pg_lisp_cluster_identity" "pg_export_snapshot"
                      "current_database" "current_schema"
                      "current_catalog" "current_user" "session_user" "user")
               :test #'string=)
       (analyze-nullary-function name args))
      ((logical-helper-function-p name) (analyze-logical-helper-function name args))
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
      ((member name '("length" "char_length" "character_length" "octet_length") :test #'string=)
       (unless (and (= (length args) 1)
                    (or (member (expr-type (first args)) '(:text :varchar :name :char))
                        (and (member name '("length" "octet_length") :test #'string=)
                             (eq (expr-type (first args)) :bytea))))
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

(defun coerce-in-constant (expr type &optional (typmod -1))
  "Coerce unknown literals through the shared SQL type input functions."
  (if (in-unknown-constant-p expr)
      (list :const (sql-cast-value (second expr) :text type typmod t) :type type :typmod typmod)
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
  "Coerce unknown constants, and resolve new catalog types before value comparison.
JSON has no comparison operators, even though its stored value is text."
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
  (when (and (member (expr-type left) '(:date :timestamp :timestamptz))
             (member (expr-type right) '(:date :timestamp :timestamptz))
             (not (eq (expr-type left) (expr-type right))))
    (let ((type (cond ((or (eq (expr-type left) :timestamptz) (eq (expr-type right) :timestamptz)) :timestamptz)
                      (t :timestamp))))
      (unless (eq (expr-type left) type) (setf left (list :cast left :type type :typmod -1)))
      (unless (eq (expr-type right) type) (setf right (list :cast right :type type :typmod -1)))))
  (when (or (member (expr-type left) '(:float4 :float8))
            (member (expr-type right) '(:float4 :float8)))
    (let ((type (if (and (eq (expr-type left) :float4) (eq (expr-type right) :float4)) :float4 :float8)))
      (unless (and (member (expr-type left) '(:int2 :int4 :int8 :numeric :float4 :float8))
                   (member (expr-type right) '(:int2 :int4 :int8 :numeric :float4 :float8)))
        (ereport "42883" "operator does not exist for ~a and ~a" (expr-type left) (expr-type right)))
      (unless (eq (expr-type left) type) (setf left (list :cast left :type type :typmod -1)))
      (unless (eq (expr-type right) type) (setf right (list :cast right :type type :typmod -1)))))
  (let ((left-type (expr-type left)) (right-type (expr-type right)))
    (when (or (eq left-type :json) (eq right-type :json))
      (ereport "42883" "operator does not exist for json"))
    (unless (or (eq left-type right-type)
                (and (member left-type '(:int2 :int4 :int8 :numeric)) (member right-type '(:int2 :int4 :int8 :numeric)))
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
      (and (eq source :jsonb)
           (member target '(:bool :numeric :int2 :int4 :int8 :float4 :float8)))
      (and (member source '(:json :jsonb)) (member target '(:json :jsonb)))
      (and (eq source :bytea) (member target '(:int2 :int4 :int8)))
      (and (eq target :bytea) (member source '(:int2 :int4 :int8)))
      (and (eq source :int2vector) (sql-array-element-type target)
           (sql-cast-allowed-p :int2 (sql-array-element-type target)))
      (and (sql-array-element-type source) (sql-array-element-type target)
           (sql-cast-allowed-p (sql-array-element-type source)
                               (sql-array-element-type target)))
      (member target '(:text :varchar :name))
      (member source '(:text :varchar :name))
      (and (member source '(:int2 :int4 :int8 :oid))
           (member target '(:int2 :int4 :int8 :oid)))
      (and (member source '(:int2 :int4 :int8 :numeric :float4 :float8))
           (member target '(:int2 :int4 :int8 :numeric :float4 :float8)))
      (and (member source '(:date :timestamp :timestamptz))
           (member target '(:date :timestamp :timestamptz)))
      (and (member source '(:timestamp :timestamptz :timetz)) (eq target :time))
      (and (eq source :time) (eq target :timetz))
      (and (eq source :bit) (member target '(:int4 :int8)))
      (and (eq target :bit) (member source '(:int4 :int8)))
      (and (eq source :numeric) (member target '(:int2 :int4 :int8)))
      (and (eq source :int4) (member target '(:bool :char)))
      (and (eq target :int4) (member source '(:bool :char)))))

(defun sql-cast-integer-input (text type)
  "Read signed integer input with C pg_strtoint's radix and underscore rules.
Diagnostics retain the original TEXT, including surrounding whitespace.  As in
numutils.c, accumulation can report range before later invalid syntax, while
syntax is checked before the final signed-width range check for TYPE."
  (let* ((input (string-trim (mapcar #'code-char '(9 10 11 12 13 32)) text))
         (size (length input)) (position 0) (sign 1) (radix 10)
         (prefixed nil) (digits 0) (number 0)
         (minimum (ecase type (:int2 -32768) (:int4 +int4-min+) (:int8 +int8-min+)))
         (maximum (ecase type (:int2 32767) (:int4 +int4-max+) (:int8 +int8-max+)))
         (type-name (ecase type (:int2 "smallint") (:int4 "integer") (:int8 "bigint"))))
    (labels ((invalid ()
               (ereport "22P02" "invalid input syntax for type ~a: \"~a\"" type-name text))
             (out-of-range ()
               (ereport "22003" "value \"~a\" is out of range for type ~a" text type-name))
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
                  (when (> number (floor (- minimum) radix))
                    (out-of-range))
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
      (unless (<= minimum number maximum)
        (out-of-range))
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

(defun sql-array-space-p (character)
  (and character (find (char-code character) '(9 10 11 12 13 32))))

(defun sql-text-array-element (text position)
  "ReadArrayToken's quoted/unquoted element rules, for one-dimensional text[]."
  (let* ((length (length text))
         (quoted (and (< position length) (char= (char text position) #\")))
         (escaped nil) (end 0)
         (buffer (make-array 0 :element-type 'character :adjustable t :fill-pointer 0)))
    (when quoted (incf position))
    (loop
      (when (>= position length) (ereport "22P02" "malformed array literal: ~s" text))
      (let ((character (char text position)))
        (cond
          ((char= character #\\)
           (setf escaped t)
           (incf position)
           (when (>= position length) (ereport "22P02" "malformed array literal: ~s" text))
           (vector-push-extend (char text position) buffer)
           (setf end (length buffer)))
          ((and quoted (char= character #\"))
           (return-from sql-text-array-element (values (coerce buffer 'simple-string) (1+ position))))
          ((and (not quoted) (find character ",}"))
           (when (zerop end) (ereport "22P02" "malformed array literal: ~s" text))
           (let ((value (subseq buffer 0 end)))
             (return-from sql-text-array-element
               (values (if (and (not escaped) (string-equal value "NULL")) +null+ value)
                       position))))
          ((and (not quoted) (find character "{\""))
           (ereport "22P02" "malformed array literal: ~s" text))
          (t
           (vector-push-extend character buffer)
           (unless (and (not quoted) (sql-array-space-p character)) (setf end (length buffer))))))
      (incf position))))

(defun sql-text-array-input (text)
  "Read the supported one-dimensional array type; reject unsupported dimensions."
  (let ((position 0) (length (length text)) (values nil))
    (labels ((skip-space ()
               (loop while (and (< position length) (sql-array-space-p (char text position)))
                     do (incf position)))
             (peek () (and (< position length) (char text position)))
             (fail () (ereport "22P02" "malformed array literal: ~s" text)))
      (skip-space)
      (when (eql (peek) #\[) (ereport "0A000" "explicit array dimensions are not supported"))
      (unless (eql (peek) #\{) (fail))
      (incf position)
      (skip-space)
      (unless (eql (peek) #\})
        (loop
          (when (eql (peek) #\{) (ereport "0A000" "multidimensional arrays are not supported"))
          (multiple-value-bind (value next) (sql-text-array-element text position)
            (push value values) (setf position next))
          (skip-space)
          (cond ((eql (peek) #\}) (return))
                ((eql (peek) #\,) (incf position) (skip-space))
                (t (fail)))))
      (incf position)
      (skip-space)
      (unless (= position length) (fail))
      (coerce (nreverse values) 'vector))))

(defun sql-array-input (text element-type &optional (typmod -1))
  "Read a rectangular array literal, retaining dimensions and lower bounds.
Convert each element using ELEMENT-TYPE and TYPMOD. Explicit bounds must
match the contents; empty arrays have no dimensions. At most six dimensions
are supported."
  (let ((position 0) (size (length text)) (bounds nil) (values nil) (item-count 0))
    (labels ((space () (loop while (and (< position size) (sql-array-space-p (char text position)))
                             do (incf position)))
             (peek () (and (< position size) (char text position)))
             (fail () (ereport "22P02" "malformed array literal: ~s" text))
             (integer-part ()
               (let ((start position))
                 (when (member (peek) '(#\+ #\-)) (incf position))
                 (loop while (and (peek) (digit-char-p (peek))) do (incf position))
                 (or (ignore-errors (parse-integer text :start start :end position)) (fail))))
             (read-level (depth)
               (when (> depth 6)
                 (ereport "54000" "number of array dimensions exceeds the maximum allowed"))
               (unless (eql (peek) #\{) (fail))
               (incf position) (space)
               (when (eql (peek) #\}) (incf position) (return-from read-level nil))
               (let ((count 0) (child-shape nil) (nested (eql (peek) #\{)))
                 (loop
                   (if nested
                       (let ((shape (read-level (1+ depth))))
                         (when (and (plusp count) (not (equal shape child-shape))) (fail))
                         (setf child-shape shape))
                       (progn
                         (when (eql (peek) #\{) (fail))
                         (multiple-value-bind (value next) (sql-text-array-element text position)
                           (when (and (member element-type '(:bytea :json :jsonb))
                                      (> (incf item-count) +max-varlena-array-elements+))
                             (ereport "54000" "array element count exceeds supported maximum"))
                           (push (sql-cast-value value :text element-type typmod) values)
                           (setf position next))))
                   (incf count) (space)
                   (cond ((eql (peek) #\}) (incf position) (return))
                         ((eql (peek) #\,) (incf position) (space))
                         (t (fail))))
                 (cons count child-shape))))
      (space)
      (loop while (eql (peek) #\[) do
        (incf position)
        (let ((first (integer-part)))
          (multiple-value-bind (lower upper)
              (if (eql (peek) #\:)
                  (progn (incf position) (values first (integer-part)))
                  (values 1 first))
            (unless (eql (peek) #\]) (fail)) (incf position) (space)
            (unless (and (<= +int4-min+ lower +int4-max+)
                         (<= +int4-min+ upper (1- +int4-max+))
                         (<= (1+ (- upper lower)) +int4-max+))
              (ereport "54000" "array dimensions exceed the maximum allowed"))
            (unless (<= lower upper) (ereport "2202E" "invalid array dimensions"))
            (push (cons lower upper) bounds)
            (when (> (length bounds) 6) (ereport "54000" "number of array dimensions exceeds the maximum allowed")))))
      (when bounds (unless (eql (peek) #\=) (fail)) (incf position))
      (space)
      (let* ((dimensions (read-level 1)) (declared (nreverse bounds)))
        (space) (unless (= position size) (fail))
        (when (> (length dimensions) 6)
          (ereport "54000" "number of array dimensions exceeds the maximum allowed"))
        (when (null values) (setf dimensions nil))
        (when declared
          (unless (equal dimensions (mapcar (lambda (bound) (1+ (- (cdr bound) (car bound)))) declared))
            (fail)))
        (make-sql-array :element-type-id element-type :elements (coerce (nreverse values) 'vector)
                        :dimensions dimensions
                        :lower-bounds (if declared (mapcar #'car declared)
                                          (make-list (length dimensions) :initial-element 1)))))))

(defun sql-session-date-order ()
  "Return the current DateStyle input field order."
  (let ((setting (if *backend* (sql-setting-value "datestyle") "ISO, MDY")))
    (cond ((search "DMY" setting :test #'char-equal) :dmy)
          ((search "YMD" setting :test #'char-equal) :ymd)
          (t :mdy))))

(defun sql-session-date-style ()
  "Return the output style selected by DateStyle."
  (let ((setting (if *backend* (sql-setting-value "datestyle") "ISO, MDY")))
    (cond ((search "German" setting :test #'char-equal) :german)
          ((search "Postgres" setting :test #'char-equal) :postgres)
          ((search "SQL" setting :test #'char-equal) :sql)
          (t :iso))))

(defun sql-session-timezone ()
  "Resolve the current backend's zone, defaulting to UTC outside a session."
  (sql-load-timezone (if *backend* (sql-setting-value "timezone") "UTC")))

(defun sql-session-unix-microseconds ()
  "Return the current transaction's start instant, or wall time outside a transaction."
  (or (and *backend* (backend-transaction-start-unix-microseconds *backend*))
      (multiple-value-bind (seconds micros) (sb-ext:get-time-of-day)
        (+ (* seconds 1000000) micros))))

(defun sql-session-local-microseconds ()
  (sql-current-local-microseconds (sql-session-timezone) (sql-session-unix-microseconds)))

(defun sql-bytea-input (text)
  "Decode hex or traditional escaped bytea input into fresh octets. Hex whitespace is accepted between byte pairs; escape input accepts doubled backslashes and three-digit octal bytes."
  (let* ((input (string->bytes text))
         (length (length input))
         (out (make-array 0 :element-type '(unsigned-byte 8)
                            :adjustable t :fill-pointer 0)))
    (labels ((digit (byte base)
               (digit-char-p (code-char byte) base))
             (put (byte) (vector-push-extend byte out)))
      (if (and (>= length 2) (= (aref input 0) 92) (= (aref input 1) 120))
          (loop with i = 2
                while (< i length)
                for byte = (aref input i)
                do (if (member byte '(32 9 10 13))
                       (incf i)
                       (let ((high (digit byte 16)))
                         (unless high (ereport "22023" "invalid hexadecimal digit"))
                         (incf i)
                         (when (= i length)
                           (ereport "22023" "invalid hexadecimal data: odd number of digits"))
                         (let ((low (digit (aref input i) 16)))
                           (unless low (ereport "22023" "invalid hexadecimal digit"))
                           (put (+ (* 16 high) low))
                           (incf i)))))
          (loop with i = 0
                while (< i length)
                for byte = (aref input i)
                do (cond
                     ((/= byte 92) (put byte) (incf i))
                     ((and (< (+ i 3) length)
                           (<= 48 (aref input (1+ i)) 51)
                           (<= 48 (aref input (+ i 2)) 55)
                           (<= 48 (aref input (+ i 3)) 55))
                      (put (+ (* 64 (- (aref input (1+ i)) 48))
                              (* 8 (- (aref input (+ i 2)) 48))
                              (- (aref input (+ i 3)) 48)))
                      (incf i 4))
                     ((and (< (1+ i) length) (= (aref input (1+ i)) 92))
                      (put 92) (incf i 2))
                     (t (ereport "22P02" "invalid input syntax for type bytea")))))
      (coerce out '(simple-array (unsigned-byte 8) (*))))))

(defun sql-bytea-concat-size (left-size right-size)
  "Check byteacat's varlena allocation size before constructing the result."
  (let ((allocation (+ left-size right-size 4)))
    (when (> allocation #x3fffffff)
      (ereport "XX000" "invalid memory alloc request size ~d" allocation))
    (- allocation 4)))

(defun sql-bytea-concat (left right)
  "Concatenate bytea values after enforcing PostgreSQL's allocation limit."
  (let* ((size (sql-bytea-concat-size (length left) (length right)))
         (result (make-array size :element-type '(unsigned-byte 8))))
    (replace result left)
    (replace result right :start1 (length left))
    result))

(defun sql-bytea-output (bytes &optional (format "hex"))
  "Render BYTES in PostgreSQL's hex or traditional escape format."
  (check-type bytes (vector (unsigned-byte 8)))
  (if (string= format "escape")
      (with-output-to-string (stream)
        (loop for byte across bytes
              do (cond ((= byte 92)
                        (write-char #\\ stream) (write-char #\\ stream))
                       ((or (< byte 32) (> byte 126))
                        (write-char #\\ stream)
                        (write-char (digit-char (ldb (byte 2 6) byte)) stream)
                        (write-char (digit-char (ldb (byte 3 3) byte)) stream)
                        (write-char (digit-char (ldb (byte 3 0) byte)) stream))
                       (t (write-char (code-char byte) stream)))))
      (let* ((digits "0123456789abcdef")
             (result (make-string (+ 2 (* 2 (length bytes))))))
        (setf (aref result 0) (code-char 92) (aref result 1) #\x)
        (loop for byte across bytes
              for i from 2 by 2
              do (setf (aref result i) (aref digits (ash byte -4))
                       (aref result (1+ i)) (aref digits (logand byte 15))))
        result)))

(defun sql-integer-to-bytea (value type)
  "Encode a signed SQL integer as its fixed-width network byte representation."
  (let* ((size (ecase type (:int2 2) (:int4 4) (:int8 8)))
         (out (make-array size :element-type '(unsigned-byte 8))))
    (dotimes (i size out)
      (setf (aref out i) (ldb (byte 8 (* 8 (- size i 1))) value)))))

(defun sql-bytea-to-integer (value type)
  "Decode up to the target width in big-endian order, matching bytea_intN. Short inputs are zero-extended; only a full-width high bit makes the result negative. Longer inputs signal an out-of-range error."
  (let* ((size (ecase type (:int2 2) (:int4 4) (:int8 8)))
         (bits (* size 8)))
    (when (> (length value) size)
      (ereport "22003" "~a out of range"
               (ecase type (:int2 "smallint") (:int4 "integer") (:int8 "bigint"))))
    (let ((number (loop for byte across value for n = byte then (+ (ash n 8) byte)
                        finally (return (or n 0)))))
      (if (logbitp (1- bits) number) (- number (ash 1 bits)) number))))

(defun sql-cast-value (value source target typmod &optional interval-literal-p)
  (when (eq value +null+) (return-from sql-cast-value +null+))
  (when (and (eq source :jsonb)
             (member target '(:bool :numeric :int2 :int4 :int8 :float4 :float8)))
    (let ((scalar (sql-jsonb-value value)))
      (return-from sql-cast-value
        (cond ((eq scalar :json-null) +null+)
              ((eq target :bool)
               (case scalar (:json-true t) (:json-false nil)
                     (otherwise (ereport "22023" "cannot cast jsonb value to boolean"))))
              ((sql-numeric-p scalar)
               (sql-cast-value scalar :numeric target typmod))
              (t (ereport "22023" "cannot cast jsonb value to ~a" target))))))
  (when (eq target :jsonb)
    (return-from sql-cast-value
      (if (eq source :jsonb) value (sql-jsonb-input (value->text value source)))))
  (when (eq target :json)
    (return-from sql-cast-value
      (if (eq source :json) value (sql-json-input (value->text value source)))))
  (when (eq target :bytea)
    (return-from sql-cast-value
      (cond ((eq source :bytea) value)
            ((member source '(:int2 :int4 :int8)) (sql-integer-to-bytea value source))
            (t (sql-bytea-input (value->text value source))))))
  (when (and (eq source :bytea) (member target '(:int2 :int4 :int8)))
    (return-from sql-cast-value (sql-bytea-to-integer value target)))
  (when (and (eq source :bytea) (member target '(:text :varchar :name)))
    (return-from sql-cast-value
      (sql-cast-value (value->text value :bytea) :text target typmod)))
  ;; A directly cast ARRAY constructor has already been analyzed as int2[].
  ;; Keep its shape and nulls; a general array-to-int2vector cast is rejected
  ;; by analysis before this function is reached.
  (when (and (eq target :int2vector) (sql-array-p value))
    (return-from sql-cast-value value))
  ;; A binary-compatible int2[] cast retains even a zero-length dimension.
  ;; Element conversion uses array mapping, which normalizes an empty result
  ;; to zero dimensions, as in the C array coercion path.
  (when (and (eq source :int2vector) (sql-array-p value) (sql-array-element-type target))
    (return-from sql-cast-value
      (make-sql-array :element-type-id (sql-array-element-type target)
                      :elements (map 'vector (lambda (element)
                                               (sql-cast-value element :int2 (sql-array-element-type target) typmod))
                                     (sql-array-elements value))
                      :dimensions (unless (and (zerop (length (sql-array-elements value)))
                                               (not (eq (sql-array-element-type target) :int2)))
                                    (sql-array-dimensions value))
                      :lower-bounds (unless (and (zerop (length (sql-array-elements value)))
                                                 (not (eq (sql-array-element-type target) :int2)))
                                      (sql-array-lower-bounds value)))))
  (when (and (eq source :int2vector) (sql-array-element-type target))
    (return-from sql-cast-value
      (make-sql-array :element-type-id (sql-array-element-type target)
                      :elements (map 'vector (lambda (element)
                                               (sql-cast-value element :int2 (sql-array-element-type target) typmod)) value)
                      :dimensions (unless (and (zerop (length value))
                                               (not (eq (sql-array-element-type target) :int2)))
                                    (list (length value)))
                      :lower-bounds (unless (and (zerop (length value))
                                                 (not (eq (sql-array-element-type target) :int2)))
                                      (list 0)))))
  (when (sql-array-element-type target)
    (let ((element-type (sql-array-element-type target)))
      (return-from sql-cast-value
        (if (sql-array-p value)
            (make-sql-array :element-type-id element-type
                            :elements (map 'vector (lambda (element)
                                                     (sql-cast-value element (sql-array-element-type source) element-type typmod))
                                           (sql-array-elements value))
                            :dimensions (unless (and (zerop (length (sql-array-elements value)))
                                                     (not (eq (sql-array-element-type source) element-type)))
                                          (sql-array-dimensions value))
                            :lower-bounds (unless (and (zerop (length (sql-array-elements value)))
                                                       (not (eq (sql-array-element-type source) element-type)))
                                            (sql-array-lower-bounds value)))
            (sql-array-input value element-type typmod)))))
  (when (member target '(:float4 :float8))
    (return-from sql-cast-value
      (cond ((member source '(:text :varchar :name)) (sql-float-input value target))
            ((sql-numeric-special-p value) (sql-float-input (sql-numeric-output value) target))
            (t (sql-float-from-number value target)))))
  (when (and (member source '(:float4 :float8)) (member target '(:int2 :int4 :int8)))
    (when (or (sb-ext:float-nan-p value) (sb-ext:float-infinity-p value))
      (ereport "22003" "~a out of range" (case target (:int2 "smallint") (:int8 "bigint") (t "integer"))))
    (return-from sql-cast-value (sql-cast-value (round value) :int8 target typmod)))
  (when (and (eq source :timestamptz) (member target '(:date :time :timestamp)))
    (setf value (sql-timezone-instant-to-local (sql-session-timezone) value))
    (when (and (eq target :timestamp)
               (not (member value '(#x7fffffffffffffff -9223372036854775808)))
               (not (<= -211813488000000000 value 9223371331199999999)))
      (ereport "22008" "timestamp out of range")))
  (when (and (eq target :date) (member source '(:timestamp :timestamptz)))
    (return-from sql-cast-value (sql-timestamp-to-date value)))
  (when (eq target :interval)
    (return-from sql-cast-value
      (sql-interval-adjust-typmod
       (if (member source '(:text :varchar :name))
           (sql-interval-input value (if interval-literal-p typmod -1)) value)
       typmod)))
  (when (member target '(:timestamp :timestamptz))
    (return-from sql-cast-value
      (if (member source '(:text :varchar :name))
          (sql-timestamp-input value typmod (eq target :timestamptz)
                               (sql-session-timezone) (sql-session-date-order)
                               (sql-session-unix-microseconds))
          (let* ((local (if (eq source :date) (sql-date-to-timestamp value) value))
                 (timestamp (if (and (eq target :timestamptz) (member source '(:date :timestamp)))
                                (sql-timezone-local-to-instant (sql-session-timezone) local)
                                local)))
            (when (and (eq target :timestamptz) (member source '(:date :timestamp))
                       (not (member timestamp '(#x7fffffffffffffff -9223372036854775808)))
                       (not (<= -211813488000000000 timestamp 9223371331199999999)))
              (ereport "22008" "timestamp out of range"))
            (if (member timestamp '(#x7fffffffffffffff -9223372036854775808)) timestamp
                (sql-time-adjust-typmod timestamp typmod))))))
  (when (and (eq source :time) (eq target :timetz))
    (return-from sql-cast-value
      (sql-timetz-input (sql-time-output value) typmod (sql-session-timezone)
                        nil (sql-session-unix-microseconds))))
  (when (and (eq source :timetz) (eq target :time))
    (return-from sql-cast-value (sql-time-adjust-typmod (sql-timetz-microseconds value) typmod)))
  (when (eq target :timetz)
    (return-from sql-cast-value
      (if (member source '(:text :varchar :name)) (sql-timetz-input value typmod (sql-session-timezone)
                                                                    (when *backend* (backend-transaction-start-unix *backend*))
                                                                    (sql-session-unix-microseconds))
          (make-sql-timetz (sql-time-adjust-typmod (sql-timetz-microseconds value) typmod)
                           (sql-timetz-zone-west value)))))
  (when (eq target :time)
    (return-from sql-cast-value
      (cond ((member source '(:text :varchar :name)) (sql-time-input value typmod (sql-session-local-microseconds)))
            ((member source '(:timestamp :timestamptz))
             (when (member value '(#x7fffffffffffffff -9223372036854775808))
               (ereport "22008" "cannot convert infinite timestamp to time"))
             (sql-time-adjust-typmod (mod value +sql-microseconds-per-day+) typmod))
            (t (sql-time-adjust-typmod value typmod)))))
  (when (eq target :numeric)
    (return-from sql-cast-value
      (sql-numeric-apply-typmod
       (cond ((member source '(:text :varchar :name)) (sql-numeric-input value))
             ((member source '(:float4 :float8)) (sql-float-to-numeric value source))
             (t value))
       typmod)))
  (when (eq target :bit)
    (return-from sql-cast-value
      (cond ((eq source :bit) (sql-bit-resize value typmod))
            ((member source '(:int4 :int8)) (sql-integer-to-bit value typmod))
            ((member source '(:text :varchar :name))
             (sql-bit-resize (sql-bit-input value) typmod))
            (t (ereport "42846" "cannot cast type ~a to bit" source)))))
  (when (and (eq source :bit) (member target '(:int4 :int8)))
    (return-from sql-cast-value (sql-bit-to-integer value (if (eq target :int4) 32 64))))
  ;; Numeric casts round before checking int2 range; float casts above round
  ;; through int8, while integer casts check their value directly.  Text uses
  ;; the int2 input function and its own syntax and range diagnostics.
  (when (eq target :int2)
    (when (member source '(:text :varchar :name))
      (return-from sql-cast-value (sql-cast-integer-input value :int2)))
    (let ((number (cond ((eq source :numeric)
                         (when (sql-numeric-special-p value)
                           (ereport "0A000" "cannot convert ~a to smallint"
                                    (if (eq value :numeric-nan) "NaN" "infinity")))
                         (sql-numeric-rounded-coefficient value 0))
                        (t value))))
      (unless (<= -32768 number 32767)
        (ereport "22003" "smallint out of range"))
      (return-from sql-cast-value number)))
  (when (and (eq source :numeric) (member target '(:int4 :int8)))
    (when (sql-numeric-special-p value)
      (ereport "0A000" "cannot convert numeric special value to integer"))
    (setf value (sql-numeric-rounded-coefficient value 0))
    (unless (if (eq target :int4) (<= +int4-min+ value +int4-max+)
                (<= +int8-min+ value +int8-max+))
      (ereport "22003" "~a out of range" (if (eq target :int4) "integer" "bigint"))))
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
       (:date (sql-date-input value nil (sql-session-date-order)
                              (floor (sql-session-local-microseconds) +sql-microseconds-per-day+)))
       (:numeric (sql-numeric-input value))
       (:char (sql-char-input value))
       (:int2vector (sql-int2vector-input value))
       (:text-array (sql-text-array-input value))
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
           (if (and (or (sql-array-element-type target) (eq target :int2vector))
                    (eq (first raw) :array-literal))
               (analyze-array-literal raw scope (if (eq target :int2vector) :int2
                                                    (sql-array-element-type target)))
               (analyze-expr raw scope)))
         (source (expr-type argument))
         (unknown-literal (and (eq (first raw) :const)
                               (or (stringp (second raw)) (eq (second raw) +null+)))))
    (sql-expression-type-description target)
    (unless (or unknown-literal
                (and (eq target :int2vector) (eq (first raw) :array-literal))
                (sql-cast-allowed-p source target))
      (ereport "42846" "cannot cast type ~a to ~a" source target))
    (cond
      (unknown-literal
       (list :const (sql-cast-value (second raw) source target typmod t) :explicit-cast t
             :type target :typmod typmod))
      ((and (eq source target) (= typmod -1) (not (eq target :bit)))
       (when (and (eq (first argument) :const) (member target '(:int2 :int4 :int8)))
         (sql-cast-value (second argument) source target typmod))
       argument)
      (t (list :cast argument :type target :typmod typmod)))))

(defun analyze-text-concatenation (left right)
  "Resolve textcat and text/anynonarray overloads without treating arrays as text."
  (when (and (or (and (eq (expr-type left) :jsonb) (not (in-unknown-constant-p left)))
                 (and (eq (expr-type right) :jsonb) (not (in-unknown-constant-p right))))
             (every (lambda (argument)
                      (or (in-unknown-constant-p argument)
                          (eq (expr-type argument) :jsonb)))
                    (list left right)))
    (return-from analyze-text-concatenation
      (list :concat (coerce-common-expression left :jsonb)
            (coerce-common-expression right :jsonb) :type :jsonb :typmod -1)))
  (when (and (or (and (eq (expr-type left) :bytea) (not (in-unknown-constant-p left)))
                 (and (eq (expr-type right) :bytea) (not (in-unknown-constant-p right))))
             (every (lambda (argument)
                      (or (in-unknown-constant-p argument)
                          (eq (expr-type argument) :bytea)))
                    (list left right)))
    (return-from analyze-text-concatenation
      (list :concat (coerce-common-expression left :bytea)
            (coerce-common-expression right :bytea) :type :bytea :typmod -1)))
  (when (and (eq (expr-type left) :bit) (eq (expr-type right) :bit))
    (ereport "0A000" "bit concatenation is not implemented"))
  (when (some (lambda (argument) (or (eq (expr-type argument) :int2vector)
                                     (sql-array-element-type (expr-type argument))))
              (list left right))
    (if (or (member (expr-type left) '(:bytea :jsonb))
            (member (expr-type right) '(:bytea :jsonb)))
        (report-operator-resolution-error "||" (list left right))
        (ereport "0A000" "array concatenation is not implemented")))
  (unless (some (lambda (argument) (sql-implicit-coercion-p (expr-type argument) :text))
                (list left right))
    (ereport "42883" "operator does not exist: ~a || ~a"
             (typed-arg-name left) (typed-arg-name right)))
  (list :concat (cast-expression-to-text left) (cast-expression-to-text right)
        :type :text :typmod -1))

(defun report-operator-resolution-error (operator arguments &optional ambiguous-p)
  "Report C op_error's signature and cast advice without coercing operands."
  (let* ((unary-p (= (length arguments) 1))
         (types (mapcar (lambda (argument)
                          (if (in-unknown-constant-p argument)
                              "unknown" (typed-arg-name argument))) arguments))
         (signature (if unary-p (cons operator types)
                        (list (first types) operator (second types)))))
    (ereport (if ambiguous-p "42725" "42883") "~a: ~{~a~^ ~}"
             (if ambiguous-p "operator is not unique" "operator does not exist") signature
             :detail (cond (ambiguous-p "Could not choose a best candidate operator.")
                           (unary-p "No operator of that name accepts the given argument type.")
                           (t "No operator of that name accepts the given argument types."))
             :hint (if (and unary-p (not ambiguous-p))
                       "You might need to add an explicit type cast."
                       "You might need to add explicit type casts."))))

(defun analyze-integer-bitwise (operator left right)
  "Resolve supported integer bitwise operators and coerce their operands.
Same-width catalog overloads choose the wider integer for mixed operands. Shifts
keep the left type and take int4 counts; unresolved unknowns can be ambiguous."
  (when (member operator '("&" "|" "#" "<<" ">>") :test #'string=)
    (let* ((shift (member operator '("<<" ">>") :test #'string=))
           (ltype (expr-type left)) (rtype (expr-type right)))
      (unless (and (or (in-unknown-constant-p left) (member ltype '(:int2 :int4 :int8)))
                   (or (in-unknown-constant-p right)
                       (member rtype (if shift '(:int2 :int4) '(:int2 :int4 :int8)))))
        (report-operator-resolution-error operator (list left right)))
      (when (and (in-unknown-constant-p left)
                 (or (in-unknown-constant-p right) (and shift (eq rtype :int2))))
        (report-operator-resolution-error operator (list left right) t))
      (when (in-unknown-constant-p left)
        (setf ltype (if (and (not shift) (member rtype '(:int2 :int4 :int8))) rtype :int4)
              left (coerce-in-constant left ltype)))
      (when (in-unknown-constant-p right)
        (setf rtype (if shift :int4 ltype)
              right (coerce-in-constant right rtype)))
      (unless (and (member ltype '(:int2 :int4 :int8))
                   (member rtype (if shift '(:int2 :int4) '(:int2 :int4 :int8))))
        (report-operator-resolution-error operator (list left right)))
      (let ((type (if shift ltype
                      (cond ((or (eq ltype :int8) (eq rtype :int8)) :int8)
                            ((or (eq ltype :int4) (eq rtype :int4)) :int4)
                            (t :int2)))))
        (setf left (coerce-common-expression left type)
              right (coerce-common-expression right (if shift :int4 type)))
        (return-from analyze-integer-bitwise
          (list :binop operator left right :type type :typmod -1))))))

(defun analyze-remainder-expression (left right)
  "Resolve the supported pg_operator remainder overloads before execution.
As in parse_oper.c's binary_oper_exact, one unknown takes its known neighbor's
type. Two unknowns are ambiguous; mixed integers widen, and numeric accepts
integer inputs. This covers integer and numeric overloads, not generic operator
candidate selection."
  (let ((arguments (list left right)))
    (unless (every (lambda (argument)
                     (or (in-unknown-constant-p argument)
                         (member (expr-type argument) '(:int2 :int4 :int8 :numeric))))
                   arguments)
      (report-operator-resolution-error "%" arguments))
    (when (every #'in-unknown-constant-p arguments)
      (report-operator-resolution-error "%" arguments t))
    (let* ((known (remove-if #'in-unknown-constant-p arguments))
           (type (cond ((find :numeric known :key #'expr-type) :numeric)
                       ((find :int8 known :key #'expr-type) :int8)
                       ((find :int4 known :key #'expr-type) :int4)
                       (t :int2))))
      (list :binop "%" (coerce-common-expression left type)
            (coerce-common-expression right type) :type type :typmod -1))))

(defun analyze-json-extraction (operator left right)
  "Resolve JSON and JSONB field and element extraction operand types."
  (unless (member (expr-type left) '(:json :jsonb))
    (report-operator-resolution-error
     operator (list left right)
     (and (in-unknown-constant-p left)
          (or (in-unknown-constant-p right)
              (or (sql-implicit-coercion-p (expr-type right) :text)
                  (member (expr-type right) '(:int2 :int4)))))))
  (cond
    ((in-unknown-constant-p right) (setf right (coerce-in-constant right :text)))
    ((and (not (eq (expr-type right) :text))
          (sql-implicit-coercion-p (expr-type right) :text))
     (setf right (list :cast right :type :text :typmod -1)))
    ((eq (expr-type right) :int2)
     (setf right (list :cast right :type :int4 :typmod -1))))
  (unless (member (expr-type right) '(:text :int4))
    (report-operator-resolution-error operator (list left right)))
  (list :binop operator left right :type (if (string= operator "->") (expr-type left) :text)
        :typmod -1))

(defun analyze-json-path-extraction (operator left right)
  "Resolve JSON and JSONB path extraction against text arrays."
  (unless (member (expr-type left) '(:json :jsonb))
    (report-operator-resolution-error
     operator (list left right)
     (and (in-unknown-constant-p left)
          (or (in-unknown-constant-p right)
              (sql-implicit-coercion-p (expr-type right) :text-array)))))
  (when (in-unknown-constant-p right)
    (setf right (coerce-in-constant right :text-array)))
  (unless (eq (expr-type right) :text-array)
    (if (sql-implicit-coercion-p (expr-type right) :text-array)
        (setf right (list :cast right :type :text-array :typmod -1))
        (report-operator-resolution-error operator (list left right))))
  (list :binop operator left right :type (if (string= operator "#>") (expr-type left) :text)
        :typmod -1))

(defun analyze-jsonb-delete (operator left right)
  "Resolve JSONB deletion without changing numeric subtraction."
  (unless (or (eq (expr-type left) :jsonb) (in-unknown-constant-p left))
    (report-operator-resolution-error operator (list left right)))
  (let ((target
          (cond
            ((string= operator "#-") :text-array)
            ((in-unknown-constant-p right) :text)
            ((member (expr-type right) '(:int2 :int4)) :int4)
            ((sql-implicit-coercion-p (expr-type right) :text-array) :text-array)
            ((sql-implicit-coercion-p (expr-type right) :text) :text)
            (t nil))))
    (unless (and target (or (in-unknown-constant-p right)
                            (eq (expr-type right) target)
                            (sql-implicit-coercion-p (expr-type right) target)))
      (report-operator-resolution-error operator (list left right)))
    (when (in-unknown-constant-p left)
      (setf left (coerce-in-constant left :jsonb)))
    (list :binop operator left (coerce-common-expression right target) :type :jsonb :typmod -1)))

(defun analyze-jsonb-predicate (operator left right)
  "Resolve JSONB containment and top-level existence operators."
  (let ((original-arguments (list left right))
        (contain-p (member operator '("@>" "<@") :test #'string=))
        (array-p (member operator '("?|" "?&") :test #'string=)))
    (when (and (in-unknown-constant-p left) (in-unknown-constant-p right)
               (or contain-p (string= operator "?|")))
      (report-operator-resolution-error operator (list left right) t))
    (when (and (in-unknown-constant-p left) (not contain-p)
               (not (or (in-unknown-constant-p right)
                        (if array-p
                            (sql-implicit-coercion-p (expr-type right) :text-array)
                            (sql-implicit-coercion-p (expr-type right) :text)))))
      (report-operator-resolution-error operator (list left right)))
    (when (and (in-unknown-constant-p left)
               (or (not contain-p) (eq (expr-type right) :jsonb)))
      (setf left (coerce-in-constant left :jsonb)))
    (when (and (in-unknown-constant-p right) (eq (expr-type left) :jsonb))
      (setf right (coerce-in-constant right (cond (contain-p :jsonb)
                                                  (array-p :text-array)
                                                  (t :text)))))
    (when (and (not contain-p) (not array-p)
               (not (eq (expr-type right) :text))
               (sql-implicit-coercion-p (expr-type right) :text))
      (setf right (list :cast right :type :text :typmod -1)))
    (when (and array-p (not (eq (expr-type right) :text-array))
               (sql-implicit-coercion-p (expr-type right) :text-array))
      (setf right (list :cast right :type :text-array :typmod -1)))
    (unless (and (eq (expr-type left) :jsonb)
                 (eq (expr-type right) (cond (contain-p :jsonb)
                                             (array-p :text-array)
                                             (t :text))))
      (report-operator-resolution-error operator original-arguments))
    (list :binop operator left right :type :bool :typmod -1)))

(defun analyze-binop-expression (expression scope)
  (let* ((operator (second expression))
         (left (analyze-expr (third expression) scope))
         (right (analyze-expr (fourth expression) scope)))
    (when (and (> (length operator) 1) (char= (char operator 0) #\^))
      (ereport "42883" "operator does not exist: ~a ~a ~a"
               (typed-arg-name left) operator (typed-arg-name right)))
    (when (and (string= operator "-") (in-unknown-constant-p left)
               (in-unknown-constant-p right))
      (report-operator-resolution-error operator (list left right) t))
    (when (and (string= operator "-") (in-unknown-constant-p left)
               (eq (expr-type right) :jsonb))
      (report-operator-resolution-error operator (list left right)))
    (when (or (string= operator "#-")
              (and (string= operator "-")
                   (or (eq (expr-type left) :jsonb)
                       (and (in-unknown-constant-p left)
                            (not (in-unknown-constant-p right))
                            (or (sql-implicit-coercion-p (expr-type right) :text)
                                (sql-implicit-coercion-p (expr-type right) :text-array))))))
      (return-from analyze-binop-expression (analyze-jsonb-delete operator left right)))
    (when (member operator '("@>" "<@" "?" "?|" "?&") :test #'string=)
      (return-from analyze-binop-expression (analyze-jsonb-predicate operator left right)))
    (when (member operator '("#>" "#>>") :test #'string=)
      (return-from analyze-binop-expression (analyze-json-path-extraction operator left right)))
    (when (member operator '("->" "->>") :test #'string=)
      (return-from analyze-binop-expression (analyze-json-extraction operator left right)))
    (when (string= operator "^")
      ;; Resolve the operator before coercing unknowns; retain op_error's
      ;; diagnostics rather than reporting the corresponding power function.
      (unless (every (lambda (argument)
                       (or (in-unknown-constant-p argument)
                           (member (expr-type argument)
                                   '(:int2 :int4 :int8 :numeric :float4 :float8))))
                     (list left right))
        (report-operator-resolution-error operator (list left right)))
      (multiple-value-bind (arguments type) (resolve-power-operands "power" (list left right))
        (return-from analyze-binop-expression
          (list :binop operator (first arguments) (second arguments)
                :type type :typmod -1))))
    (when (member operator '("&" "|" "#" "<<" ">>") :test #'string=)
      (return-from analyze-binop-expression (analyze-integer-bitwise operator left right)))
    (unless (member operator '("||" "+" "-" "*" "/" "%" "=" "<>" "<" "<=" ">" ">=") :test #'string=)
      (ereport "42883" "operator does not exist: ~a ~a ~a"
               (typed-arg-name left) operator (typed-arg-name right)))
    (when (string= operator "||")
      (return-from analyze-binop-expression (analyze-text-concatenation left right)))
    (when (string= operator "%")
      (return-from analyze-binop-expression (analyze-remainder-expression left right)))
    (if (member operator '("=" "<>" "<" "<=" ">" ">=") :test #'string=)
        (let ((operands (analyze-comparison-operands left right)))
          (list :binop operator (first operands) (second operands) :type :bool :typmod -1))
        (progn
          (when (or (member (expr-type left) '(:time :timestamp :timestamptz :interval :timetz))
                    (member (expr-type right) '(:time :timestamp :timestamptz :interval :timetz)))
            (when (and (eq (expr-type left) :interval) (member operator '("*" "/") :test #'string=))
              (when (in-unknown-constant-p right) (setf right (coerce-in-constant right :float8)))
              (when (member (expr-type right) '(:int2 :int4 :int8 :numeric :float4))
                (setf right (list :cast right :type :float8 :typmod -1))))
            (when (and (eq (expr-type right) :interval) (string= operator "*"))
              (when (in-unknown-constant-p left) (setf left (coerce-in-constant left :float8)))
              (when (member (expr-type left) '(:int2 :int4 :int8 :numeric :float4))
                (setf left (list :cast left :type :float8 :typmod -1))))
            (let* ((ltype (expr-type left)) (rtype (expr-type right))
                                            (type (cond
                                                    ((and (eq ltype :interval) (eq rtype :float8)
                                                          (member operator '("*" "/") :test #'string=)) :interval)
                                                    ((and (eq ltype :float8) (eq rtype :interval) (string= operator "*")) :interval)
                                                    ((and (eq ltype :interval) (eq rtype :interval)
                                                          (member operator '("+" "-") :test #'string=)) :interval)
                                                    ((and (eq ltype :time) (eq rtype :time) (string= operator "-")) :interval)
                                                    ((and (member ltype '(:timestamp :timestamptz)) (eq ltype rtype) (string= operator "-")) :interval)
                                                    ((and (member ltype '(:date :time :timestamp :timestamptz)) (eq rtype :interval)
                                                          (member operator '("+" "-") :test #'string=)) (if (eq ltype :date) :timestamp ltype))
                                                    ((and (eq ltype :interval) (member rtype '(:date :time :timestamp :timestamptz))
                                                          (string= operator "+")) (if (eq rtype :date) :timestamp rtype)))))
              (unless type (ereport "42883" "operator ~a does not exist for ~a and ~a" operator ltype rtype))
              (return-from analyze-binop-expression (list :binop operator left right :type type :typmod -1))))
          (when (or (eq (expr-type left) :date) (eq (expr-type right) :date))
            (let* ((left-type (expr-type left)) (right-type (expr-type right))
                                                (date-difference (and (eq left-type :date) (eq right-type :date) (string= operator "-")))
                                                (date-offset (or (and (eq left-type :date) (member right-type '(:int2 :int4))
                                                                      (member operator '("+" "-") :test #'string=))
                                                                 (and (member left-type '(:int2 :int4)) (eq right-type :date)
                                                                      (string= operator "+")))))
              (unless (or date-difference date-offset)
                (ereport "42883" "operator ~a does not exist for ~a and ~a" operator left-type right-type))
              (return-from analyze-binop-expression
                (list :binop operator left right :type (if date-difference :int4 :date) :typmod -1))))
          ;; binary_oper_exact uses the known integer type for an unknown literal.
          (when (and (string= operator "-") (in-unknown-constant-p left)
                     (member (expr-type right) '(:int2 :int4 :int8)))
            (setf left (coerce-in-constant left (expr-type right))))
          (when (and (string= operator "-") (in-unknown-constant-p right)
                     (member (expr-type left) '(:int2 :int4 :int8)))
            (setf right (coerce-in-constant right (expr-type left))))
          (when (and (in-unknown-constant-p left) (member (expr-type right) '(:float4 :float8)))
            (setf left (coerce-in-constant left (expr-type right))))
          (when (and (in-unknown-constant-p right) (member (expr-type left) '(:float4 :float8)))
            (setf right (coerce-in-constant right (expr-type left))))
          (when (or (member (expr-type left) '(:float4 :float8))
                    (member (expr-type right) '(:float4 :float8)))
            (unless (member operator '("+" "-" "*" "/") :test #'string=)
              (ereport "42883" "operator ~a does not exist for floating-point operands" operator))
            (let ((type (if (and (eq (expr-type left) :float4) (eq (expr-type right) :float4)) :float4 :float8)))
              (unless (and (member (expr-type left) '(:int2 :int4 :int8 :numeric :float4 :float8))
                           (member (expr-type right) '(:int2 :int4 :int8 :numeric :float4 :float8)))
                (ereport "42883" "float operator requires numeric operands"))
              (unless (eq (expr-type left) type) (setf left (list :cast left :type type :typmod -1)))
              (unless (eq (expr-type right) type) (setf right (list :cast right :type type :typmod -1)))
              (return-from analyze-binop-expression
                (list :binop operator left right :type type :typmod -1))))
          (when (or (eq (expr-type left) :numeric) (eq (expr-type right) :numeric))
            ;; Like binary_oper_exact, use the known numeric operand's type for
            ;; an unknown literal before applying the supported numeric operator.
            (when (and (in-unknown-constant-p left) (eq (expr-type right) :numeric))
              (setf left (coerce-in-constant left :numeric)))
            (when (and (in-unknown-constant-p right) (eq (expr-type left) :numeric))
              (setf right (coerce-in-constant right :numeric)))
            (unless (and (member (expr-type left) '(:int2 :int4 :int8 :numeric))
                         (member (expr-type right) '(:int2 :int4 :int8 :numeric)))
              (ereport "42883" "numeric operator requires numeric operands"))
            (return-from analyze-binop-expression
              (list :binop operator left right :type :numeric :typmod -1)))
          (when (or (eq (expr-type left) :pg-lsn) (eq (expr-type right) :pg-lsn))
            (ereport "0A000" "pg_lsn arithmetic is not implemented"))
          (when (or (sql-array-element-type (expr-type left))
                    (sql-array-element-type (expr-type right))
                    (member (expr-type left) '(:char :int2vector :oid :bit :timestamptz))
                    (member (expr-type right) '(:char :int2vector :oid :bit :timestamptz)))
            (ereport "42883" "operator ~a does not exist for ~a and ~a"
                     operator (expr-type left) (expr-type right)))
          (list :binop operator left right
                :type (cond ((or (eq (expr-type left) :int8) (eq (expr-type right) :int8)) :int8)
                            ((and (eq (expr-type left) :int2) (eq (expr-type right) :int2)) :int2)
                            (t :int4))
                :typmod -1)))))

(defun coerce-common-expression (expression type)
  (cond
    ((in-unknown-constant-p expression)
     (if *defer-function-constant-coercion-p*
         (list :cast expression :type type :typmod -1)
         (coerce-in-constant expression type)))
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

(defun common-expression-typmod (expressions)
  "Retain a typmod only when all already-coerced expressions agree."
  (let ((typmod (expr-typmod (first expressions))))
    (if (every (lambda (expression) (= typmod (expr-typmod expression))) expressions)
        typmod -1)))

(defun analyze-coalesce-expression (expression scope)
  "Match transformCoalesceExpr: resolve and coerce every argument before execution."
  (let* ((arguments (mapcar (lambda (argument) (analyze-expr argument scope))
                            (second expression)))
         (type (common-expression-type arguments "COALESCE"))
         (coerced (mapcar (lambda (argument) (coerce-common-expression argument type))
                          arguments)))
    (when (analyzed-expression-contains-set-p arguments)
      (ereport "0A000" "set-returning functions are not allowed in COALESCE"))
    (list :coalesce coerced :type type
          :typmod (common-expression-typmod coerced))))

(defun coerce-boolean-expression (expression construct)
  "Match coerce_to_boolean for supported Boolean input types and set checks."
  (let ((coerced (coerce-in-constant expression :bool)))
    (unless (eq (expr-type coerced) :bool)
      (ereport "42804" "argument of ~a must be type boolean, not type ~a"
               construct (typed-arg-name coerced)))
    (when (analyzed-expression-contains-set-p coerced)
      (ereport "42804" "argument of ~a must not return a set" construct))
    coerced))

(defun analyze-boolean-expression (expression scope)
  "Analyze all Boolean operands before the executor may skip any of them."
  (let* ((kind (first expression))
         (construct (symbol-name kind))
         (operands (if (eq kind :not) (list (second expression))
                       (list (second expression) (third expression)))))
    (append (list kind)
            (mapcar (lambda (operand)
                      (coerce-boolean-expression (analyze-expr operand scope) construct))
                    operands)
            (list :type :bool :typmod -1))))

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
          :typmod (common-expression-typmod coerced))))

(defun analyze-array-literal (expression scope &optional target-element)
  (let* ((raw-items (second expression))
         (items (mapcar (lambda (item)
                          (if (and target-element (eq (first item) :array-literal))
                              (analyze-array-literal item scope target-element)
                              (analyze-expr item scope))) raw-items)))
    (unless (or items target-element) (ereport "42P18" "cannot determine type of empty array"))
    (let* ((multidims (some (lambda (item) (sql-array-element-type (expr-type item))) items))
           (type (if target-element
                     (if multidims (sql-array-type target-element) target-element)
                     (common-expression-type items "ARRAY")))
           (array-type (or (and (sql-array-element-type type) type) (sql-array-type type))))
      (unless array-type (ereport "0A000" "ARRAY element type ~a is not supported" type))
      (list :array
            (mapcar (lambda (item)
                      (if (or (not target-element) (in-unknown-constant-p item)
                              (eq (expr-type item) type))
                          (coerce-common-expression item type)
                          (if (sql-cast-allowed-p (expr-type item) type)
                              (list :cast item :type type :typmod -1)
                              (ereport "42846" "cannot cast type ~a to ~a" (expr-type item) type))))
                    items)
            :type array-type :typmod -1 :multidims multidims))))

(defun analyze-array-subscript (expression scope)
  "Analyze a scalar, possibly multidimensional array reference."
  (let* ((array (analyze-expr (second expression) scope))
         (element-type (sql-array-element-type (expr-type array))))
    (unless element-type
      (ereport "42804" "cannot subscript type ~a because it does not support subscripting"
               (expr-type array)))
    (let ((indices
            (mapcar (lambda (raw)
                      (let ((index (analyze-expr raw scope)))
                        (cond ((in-unknown-constant-p index) (coerce-in-constant index :int4))
                              ((eq (expr-type index) :int4) index)
                              ((member (expr-type index) '(:int2 :int8 :oid :numeric :float4 :float8))
                               (list :cast index :type :int4 :typmod -1))
                              (t (ereport "42804" "array subscript must have type integer")))))
                    (third expression))))
      (when (> (length indices) 6)
        (ereport "54000" "number of array dimensions exceeds the maximum allowed"))
      (list :array-subscript array indices :type element-type :typmod (expr-typmod array)))))

(defun eval-array-subscript (expression env)
  "Read an array element using its actual dimensions and lower bounds."
  (let ((array (eval-expr (second expression) env)))
    (when (eq array +null+) (return-from eval-array-subscript +null+))
    (let* ((indices (mapcar (lambda (index) (eval-expr index env)) (third expression)))
           (dimensions (sql-array-dimensions array))
           (bounds (sql-array-lower-bounds array))
           (offset 0))
      (unless (and (= (length indices) (length dimensions))
                   (notany (lambda (index) (eq index +null+)) indices))
        (return-from eval-array-subscript +null+))
      (loop for index in indices for dimension in dimensions for lower in bounds
            for relative = (- index lower)
            do (unless (<= 0 relative (1- dimension))
                 (return-from eval-array-subscript +null+))
               (setf offset (+ (* offset dimension) relative)))
      (aref (sql-array-elements array) offset))))

(defun analyze-array-slice (expression scope)
  "Resolve slice bounds while retaining omitted bounds."
  (let* ((array (analyze-expr (second expression) scope))
         (element-type (sql-array-element-type (expr-type array))))
    (unless element-type
      (ereport "42804" "cannot subscript type ~a because it does not support subscripting"
               (expr-type array)))
    (let ((bounds
            (mapcar (lambda (dimension)
                      (mapcar (lambda (raw)
                                (when raw
                                  (let ((index (analyze-expr raw scope)))
                                    (cond ((in-unknown-constant-p index) (coerce-in-constant index :int4))
                                          ((eq (expr-type index) :int4) index)
                                          ((member (expr-type index) '(:int2 :int8 :oid :numeric :float4 :float8))
                                           (list :cast index :type :int4 :typmod -1))
                                          (t (ereport "42804" "array subscript must have type integer"))))))
                              dimension))
                    (third expression))))
      (when (> (length bounds) 6)
        (ereport "54000" "number of array dimensions exceeds the maximum allowed"))
      (list :array-slice array bounds :type (expr-type array) :typmod (expr-typmod array)))))

(defun eval-array-slice (expression env)
  "Return a clipped array slice with fresh one-based dimensions."
  (let ((array (eval-expr (second expression) env)))
    (when (eq array +null+) (return-from eval-array-slice +null+))
    (let* ((specs (third expression))
           (uppers (mapcar (lambda (pair) (when (second pair) (eval-expr (second pair) env))) specs))
           (lowers (mapcar (lambda (pair) (when (first pair) (eval-expr (first pair) env))) specs))
           (dimensions (sql-array-dimensions array))
           (bounds (sql-array-lower-bounds array))
           (element-type (sql-array-element-type-id array))
           (empty (lambda () (make-sql-array :element-type-id element-type
                                             :elements #() :dimensions nil :lower-bounds nil))))
      (when (or (member +null+ uppers) (member +null+ lowers))
        (return-from eval-array-slice +null+))
      (when (or (null dimensions) (> (length specs) (length dimensions)))
        (return-from eval-array-slice (funcall empty)))
      (let ((starts nil) (sizes nil))
        (loop for dim in dimensions for base in bounds for i from 0
              for low = (or (nth i lowers) base)
              for high = (or (nth i uppers) (+ base dim -1))
              for start = (max low base)
              for end = (min high (+ base dim -1))
              do (when (> start end) (return-from eval-array-slice (funcall empty)))
                 (push (- start base) starts)
                 (push (1+ (- end start)) sizes))
        (setf starts (nreverse starts) sizes (nreverse sizes))
        (let* ((count (reduce #'* sizes :initial-value 1))
               (result (make-array count))
               (source (sql-array-elements array))
               (reverse-dimensions (reverse dimensions))
               (reverse-sizes (reverse sizes))
               (reverse-starts (reverse starts)))
          (dotimes (index count)
            (when (zerop (mod index 4096)) (check-for-interrupts))
            (let ((remainder index) (offset 0) (stride 1))
              (loop for dimension in reverse-dimensions
                    for size in reverse-sizes for start in reverse-starts
                    do (multiple-value-bind (quotient coordinate) (floor remainder size)
                         (incf offset (* (+ start coordinate) stride))
                         (setf remainder quotient stride (* stride dimension))))
              (setf (aref result index) (aref source offset))))
          (make-sql-array :element-type-id element-type :elements result
                          :dimensions sizes :lower-bounds (make-list (length sizes) :initial-element 1)))))))

(defun eval-array-assignment (expression env)
  "Assign an element without mutating the input array or its element vector."
  (let* ((array (eval-expr (second expression) env))
         (indices (mapcar (lambda (index) (eval-expr index env)) (third expression))))
    (when (member +null+ indices)
      (ereport "22004" "array subscript in assignment must not be null"))
    (let* ((value (eval-expr (fourth expression) env))
           (element-type (sql-array-element-type (expr-type expression)))
           (dimensions (unless (eq array +null+) (sql-array-dimensions array)))
           (bounds (unless (eq array +null+) (sql-array-lower-bounds array))))
      (when (null dimensions)
        (when (member #x7fffffff indices)
          (ereport "54000" "array subscript out of range"))
        (return-from eval-array-assignment
          (make-sql-array :element-type-id element-type :elements (vector value)
                          :dimensions (make-list (length indices) :initial-element 1)
                          :lower-bounds (copy-list indices))))
      (unless (= (length indices) (length dimensions))
        (ereport "2202E" "wrong number of array subscripts"))
      (if (= (length dimensions) 1)
          (let* ((old-lower (first bounds)) (old-size (first dimensions))
                                            (index (first indices))
                                            (new-lower (min old-lower index))
                                            (new-upper (max (+ old-lower old-size -1) index))
                                            (size (1+ (- new-upper new-lower))))
            (when (or (> size +max-varlena-array-elements+) (> size #x7fffffff)
                      (= new-upper #x7fffffff))
              (ereport "54000" "array size exceeds the maximum allowed"))
            (let ((result (make-array size :initial-element +null+)))
              (replace result (sql-array-elements array) :start1 (- old-lower new-lower))
              (setf (aref result (- index new-lower)) value)
              (make-sql-array :element-type-id element-type :elements result
                              :dimensions (list size) :lower-bounds (list new-lower))))
          (let ((offset 0))
            (loop for index in indices for dimension in dimensions for lower in bounds
                  for relative = (- index lower)
                  do (unless (<= 0 relative (1- dimension))
                       (ereport "2202E" "array subscript out of range"))
                     (setf offset (+ (* offset dimension) relative)))
            (let ((result (copy-seq (sql-array-elements array))))
              (setf (aref result offset) value)
              (make-sql-array :element-type-id element-type :elements result
                              :dimensions (copy-list dimensions) :lower-bounds (copy-list bounds))))))))

(defun analyze-scalar-array-argument (raw scope)
  (let ((array (analyze-expr raw scope)))
    (if (and (in-unknown-constant-p array) (stringp (second array)))
        (list :const (sql-text-array-input (second array)) :type :text-array :typmod -1
              :infer-element-type t)
        array)))

(defun analyze-any-expression (expression scope)
  (let* ((left (analyze-expr (third expression) scope))
         (array (analyze-scalar-array-argument (fourth expression) scope))
         (element (or (sql-array-element-type (expr-type array))
                      (and (eq (expr-type array) :int2vector) :int2))))
    (when (and (in-unknown-constant-p array) (eq (second array) +null+))
      (return-from analyze-any-expression
        (list (first expression) (second expression) left array :type :bool :typmod -1)))
    (unless element (ereport "42809" "ANY requires a supported array argument"))
    (let ((operands
            (analyze-comparison-operands
             left (list :const +null+
                        :type (if (getf (cddr array) :infer-element-type)
                                  (expr-type left) element)
                        :typmod -1 :explicit-cast t))))
      (when (getf (cddr array) :infer-element-type)
        ;; C inputs literal arrays at analysis time, including dead CASE arms.
        (let ((target (expr-type (second operands))))
          (setf array (list :scalar-array-const
                            (map 'vector (lambda (value) (sql-cast-value value :text target -1))
                                 (second array)))
                element target)))
      (list (first expression) (second expression) (first operands) array
            :element-source element :element-target (expr-type (second operands))
            :type :bool :typmod -1))))

(defvar *query-parameter-types* nil)

(defvar *query-parameter-values* nil)

(defun query-parameter-slot (index vector)
  (unless (and (integerp index) (<= 1 index) (vectorp vector) (<= index (length vector)))
    (ereport "42P02" "there is no parameter $~a" index))
  (aref vector (1- index)))

(defun analyze-expr (expr table)
  "Resolve column references and annotate expression types."
  (case (first expr)
    (:parameter
     (let ((type (query-parameter-slot (second expr) *query-parameter-types*)))
       (unless type (ereport "42P18" "could not determine data type of parameter $~a" (second expr)))
       (list :parameter (second expr) :type type :typmod -1)))
    (:sublink (analyze-sublink expr table))
    (:window (analyze-window-call expr table))
    (:const
     (let ((v (second expr)))
       (cond
         ((eq v +null+) (list :const v :type :text :typmod -1))
         ((integerp v)
          (list :const v
                :type (cond ((<= +int4-min+ v +int4-max+) :int4)
                            ((<= +int8-min+ v +int8-max+) :int8)
                            (t :numeric))
                :typmod -1))
         ((stringp v) (list :const v :type :text :typmod -1))
         (t (list :const v :type :text :typmod -1)))))
    (:numeric-literal
     (list :const (sql-numeric-input (second expr)) :type :numeric :typmod -1))
    (:bit-literal
     (list :const (sql-bit-input (second expr)) :explicit-cast t :type :bit :typmod -1))
    (:coalesce (analyze-coalesce-expression expr table))
    (:case (analyze-case-expression expr table))
    (:array-literal (analyze-array-literal expr table))
    (:array-subscript (analyze-array-subscript expr table))
    (:array-slice (analyze-array-slice expr table))
    ((:any :all) (analyze-any-expression expr table))
    (:cast (analyze-cast-expression expr table))
    (:record-field (analyze-record-field expr table))
    (:single-record
     (let ((call (analyze-expr (second expr) table)))
       (if (eq (first call) :set-func) call
           (cons :single-record (cdr call)))))
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
       (when (member (second expr) '("@" "|/" "||/") :test #'string=)
         ;; parse_oper.c's left_oper selects a pg_operator candidate before coercion;
         ;; operand analysis comes first, and input conversion precedes execution.
         (unless (or (in-unknown-constant-p e)
                     (member (expr-type e) '(:int2 :int4 :int8 :numeric :float4 :float8)))
           (report-operator-resolution-error (second expr) (list e)))
         ;; Share function coercion while retaining operator identity for grouping.
         (let ((resolved (cond ((string= (second expr) "||/")
                                (analyze-trig-function "cbrt" (list e)))
                               ((string= (second expr) "|/")
                                (analyze-trig-function "sqrt" (list e)))
                               (t (analyze-basic-numeric-function "abs" (list e))))))
           (return-from analyze-expr
             (list :unary (second expr) (first (third resolved))
                   :type (expr-type resolved) :typmod -1))))
       (when (string= (second expr) "~")
         (when (in-unknown-constant-p e)
           (report-operator-resolution-error "~" (list e) t))
         (unless (member (expr-type e) '(:int2 :int4 :int8))
           (report-operator-resolution-error (second expr) (list e)))
         (return-from analyze-expr
           (list :unary "~" e :type (expr-type e) :typmod -1)))
       (unless (string= (second expr) "-")
         (ereport "42883" "operator does not exist: ~a ~a" (second expr) (typed-arg-name e)))
       (when (or (sql-array-element-type (expr-type e))
                 (member (expr-type e) '(:char :int2vector :oid :pg-lsn :bit :timestamptz :timestamp :date :time :timetz)))
         (ereport "42883" "unary operator does not exist for ~a" (expr-type e)))
       (list :unary (second expr) e
             :type (expr-type e) :typmod (expr-typmod e))))
    ((:and :or :not) (analyze-boolean-expression expr table))
    (:in (analyze-in-expression expr table))
    (:is-null
     (list :is-null (analyze-expr (second expr) table) (third expr)
           :type :bool :typmod -1))
    (:is-bool
     (list :is-bool (analyze-expr (second expr) table) (third expr)
           (fourth expr) :type :bool :typmod -1))
    (:func (if (raw-aggregate-kind expr)
               (progn
                 (when (getf (cdddr expr) :call-distinct)
                   (ereport "0A000" "DISTINCT function arguments are not supported"))
                 (when (getf (cdddr expr) :call-order)
                   (ereport "0A000" "aggregate ORDER BY is not supported"))
                 (analyze-aggregate-call expr table))
               ;; Lookup must fail before rejecting decorations on a scalar match.
               (let ((result (analyze-function-expression expr table)))
                 (when (getf (cdddr expr) :call-distinct)
                   (ereport "42809" "DISTINCT specified, but ~a is not an aggregate function"
                            (if (getf (cdddr expr) :schema)
                                (format nil "~a.~a" (getf (cdddr expr) :schema) (second expr))
                                (second expr))))
                 (when (getf (cdddr expr) :call-order)
                   (ereport "42809" "ORDER BY specified, but ~a is not an aggregate function"
                            (if (getf (cdddr expr) :schema)
                                (format nil "~a.~a" (getf (cdddr expr) :schema) (second expr))
                                (second expr))))
                 result)))
    (:count-star
     (analyze-aggregate-call expr table))
    (t
     (ereport "XX000" "cannot analyze expression ~a" expr))))

(defun cast-target-default-name (ast)
  ;; Preserve a stronger inner expression label before falling back to the type.
  "A cast supplies a weak type label; a column/function label takes precedence."
  (let ((argument (second ast)))
    (loop while (eq (first argument) :cast) do (setf argument (second argument)))
    (case (first argument)
      (:func (second argument))
      ((:column :count-star :coalesce :window :array-literal) (target-default-name argument))
      (t (if (eq (third ast) :pg-lsn) "pg_lsn"
             (string-downcase (symbol-name (or (sql-array-element-type (third ast)) (third ast)))))))))

(defun target-default-name (ast)
  (case (first ast)
    ((:window :array-subscript :array-slice) (target-default-name (second ast)))
    (:column
     (if (third ast) (third ast) (second ast)))
    (:array-literal "array")
    (:coalesce "coalesce")
    (:case "case")
    (:count-star "count")
    (:func (second ast))
    (:record-field (third ast))
    (:cast (cast-target-default-name ast))
    (t "?column?")))

(defun analyze-target (target table)
  (let* ((ast (getf target :expr))
         (alias (getf target :alias))
         (resolved (let ((*select-set-functions-allowed-p* t)) (analyze-expr ast table)))
         (direct-column (loop for expression = resolved then (third expression)
                              while (eq (first expression) :outer-ref)
                              finally (return (and (eq (first expression) :col) expression))))
         (type (expr-type resolved))
         (typmod (expr-typmod resolved))
         (name (or alias (subquery-default-name resolved)
                   (target-default-name ast))))
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
  (when (eq (first raw) :single-record)
    (return-from sql-composite-call-fields (sql-composite-call-fields (second raw))))
  (unless (eq (first raw) :func)
    (ereport "42809" "expression is not a supported composite type"))
  (let ((name (builtin-function-name (second raw) (getf (cdddr raw) :schema))))
    (cond ((string= name "pg_get_publication_tables") +publication-table-result-fields+)
          ((string= name "pg_stat_get_wal_receiver") +wal-receiver-result-fields+)
          ((string= name "pg_get_replication_slots") +replication-slot-result-fields+)
          ((string= name "pg_create_physical_replication_slot") +physical-slot-create-result-fields+)
          ((string= name "pg_replication_slot_advance") +physical-slot-advance-result-fields+)
          (t (ereport "42809" "expression is not a supported composite type")))))

(defun qualified-star-targets (qualifier scope)
  (loop for current = scope then (select-scope-parent current)
        while (typep current 'select-scope)
        for relation = (find qualifier (select-scope-relations current)
                             :key #'select-relation-qualifier :test #'equal)
        when relation
          do (return-from qualified-star-targets
               (loop for column in (select-relation-columns relation)
                     collect (list :expr (list :column qualifier (select-column-name column))))))
  (ereport "42P01" "missing FROM-clause entry for table ~s" qualifier))

(defun expand-select-targets (targets scope)
  "Expand records before analysis, as C ExpandRowReference does with FieldSelect."
  (loop for target in targets for raw = (getf target :expr)
        append
        (cond
          ((getf target :qualified-star)
           (qualified-star-targets (getf target :qualified-star) scope))
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
                 collect (list :expr (list :record-field
                                           (if (getf (cddr raw) :once)
                                               (list :single-record (copy-tree (second raw)))
                                               (copy-tree (second raw))) name)
                               :alias name)))
          (t (list target)))))

(defun analyzed-expression-contains-set-p (form)
  (and (consp form)
       (or (eq (first form) :set-func)
           (analyzed-expression-contains-set-p (car form))
           (analyzed-expression-contains-set-p (cdr form)))))

(defun analyze-replication-slot-call (name arguments)
  "Resolve the nullary slot SRF and reject contexts that cannot accept a set."
  (when arguments
    (ereport "42883" "function ~a(~a) does not exist" name (call-signature arguments)))
  (unless *select-set-functions-allowed-p*
    (ereport "0A000" "set-returning functions are not allowed in this context"))
  (when *inside-aggregate-input-p*
    (ereport "0A000" "aggregate function calls cannot contain set-returning functions"))
  (list :set-func name nil :type :record :typmod -1 :fields +replication-slot-result-fields+))

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

(defun analyzed-expression-identical-p (left right)
  "Compare executable expressions while preserving aggregate transition slots."
  ;; ProjectSet sharing must not merge distinct volatile aggregate transitions.
  (cond ((and (sql-jsonb-p left) (sql-jsonb-p right))
         (string= (sql-jsonb-output left) (sql-jsonb-output right)))
        ((and (sql-decimal-p left) (sql-decimal-p right))
         (and (= (sql-decimal-scale left) (sql-decimal-scale right))
              (= (sql-decimal-coefficient left) (sql-decimal-coefficient right))))
        ((and (sql-expression-key-p left) (sql-expression-key-p right))
         (equal (sql-expression-key-value left) (sql-expression-key-value right)))
        ((and (consp left) (consp right))
         (and (analyzed-expression-identical-p (car left) (car right))
              (analyzed-expression-identical-p (cdr left) (cdr right))))
        (t (equal left right))))

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
                 ((member (first form) '(:set-func :single-record))
                  (let ((position
                          (and (or reuse-p share-calls-p (eq (first form) :single-record))
                               (or (position form calls :test #'eq)
                                   (position form calls :test #'analyzed-expression-identical-p)))))
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
                                             :key (lambda (target) (getf target :expr))
                                             :test #'selected-expression-equal-p)))
                             (expression (if position
                                             (list :sort-output (1+ position)
                                                   :type (getf (nth position result-targets) :type)
                                                   :typmod (getf (nth position result-targets) :typmod))
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
  (when (and (eq (first left) :outer-ref) (eq (first right) :outer-ref))
    (return-from same-group-column-p
      (and (= (second left) (second right))
           (same-group-column-p (third left) (third right)))))
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

(defun analyze-group-keys (group-asts scope &optional targets)
  "Resolve a GROUP BY list and keep its first occurrence of each column."
  (let ((keys '()))
    (dolist (ast group-asts)
      ;; Diagnose unsupported JSON equality even when GROUP BY uses an ordinal.
      (when (and targets (eq (first ast) :const) (integerp (second ast))
                 (<= 1 (second ast) (length targets)))
        (let ((target-expr (getf (nth (1- (second ast)) targets) :expr)))
          (when (and target-expr
                     (member (expr-type (analyze-expr target-expr scope)) '(:json :json-array)))
            (ereport "42883" "could not identify an equality operator for type json"))))
      (let ((expr (analyze-expr ast scope)))
        (when (member (expr-type expr) '(:json :json-array))
          (ereport "42883" "could not identify an equality operator for type json"))
        (unless (or (eq (first expr) :col)
                    (and (eq (first expr) :outer-ref) (eq (first ast) :column)))
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
              (or (null (getf (cdddr form) :schema))
                  (string= (getf (cdddr form) :schema) "pg_catalog")))
         (cond ((string= (second form) "array_agg") :array-agg)
               ((string= (second form) "sum") :sum)
               ((string= (second form) "count") :count-expr)
               ((string= (second form) "min") :min)
               ((string= (second form) "max") :max)
               ((string= (second form) "avg") :avg)))))

(defun contains-aggregate-p (form)
  ;; Walk the full call, including ORDER BY expressions stored in its plist.
  (and (consp form)
       (if (member (first form) '(:sublink :window))
           (contains-aggregate-p (fourth form))
           (or (raw-aggregate-kind form)
               (contains-aggregate-p (car form))
               (contains-aggregate-p (cdr form))))))

(defun analyzed-expression-volatile-p (form)
  "Only known stable/immutable builtins may share an aggregate transition."
  (and (consp form)
       (or (eq (first form) :subplan)
           (and (eq (first form) :func)
                (not (or (integer-catalog-alias-p (second form))
                         (float-catalog-alias (second form))
                         (trig-function-p (second form))
                         (numeric-arithmetic-function-p (second form))
                         (numeric-helper-function-p (second form))
                         (member (second form)
                                 '("version" "pg_lisp_runtime" "current_database" "current_schema"
                                   "current_catalog" "current_user" "session_user" "user" "pg_postmaster_start_time"
                                   "json_typeof" "jsonb_typeof" "json_array_length" "jsonb_array_length" "jsonb_set" "jsonb_insert" "to_jsonb"
                                   "substr" "substring" "length" "char_length" "character_length" "octet_length" "pg_backend_pid"
                                   "abs" "numeric_abs" "sign" "round" "trunc" "ceil" "ceiling" "floor"
                                   "sqrt" "numeric_sqrt" "exp" "numeric_exp" "ln" "numeric_ln"
                                   "log" "log10" "numeric_log" "power" "pow" "numeric_power" "dpow"
                                   "scale" "min_scale" "trim_scale" "width_bucket")
                                 :test #'string=))))
           (analyzed-expression-volatile-p (car form))
           (analyzed-expression-volatile-p (cdr form)))))

(defun analyze-aggregate-input (raw scope kind)
  "Resolve supported pg_aggregate signatures before transition setup."
  (when (find :named-arg (third raw) :key (lambda (argument) (and (consp argument) (first argument))))
    (ereport "0A000" "aggregates cannot use named arguments"))
  (when (eq kind :count) (return-from analyze-aggregate-input (values nil :int8)))
  (unless (= 1 (length (third raw)))
    (ereport "42883" "function ~a requires one argument" (second raw)))
  (let* ((*inside-aggregate-input-p* t)
         (argument (analyze-expr (first (third raw)) scope))
         (type (expr-type argument)))
    (when (and (member kind '(:sum :avg :array-agg)) (in-unknown-constant-p argument))
      (ereport "42725" "function ~a(unknown) is not unique" (second raw)))
    (ecase kind
      (:count-expr (values argument :int8))
      ((:min :max)
       (unless (member type '(:int2 :int4 :int8 :numeric :float4 :float8 :text :varchar :name :oid :pg-lsn :timestamptz :timestamp :date :time :timetz :interval))
         (ereport "42883" "function ~a(~a) does not exist" (second raw) (typed-arg-name argument)))
       (values argument (if (member type '(:varchar :name)) :text type)))
      (:array-agg
       (unless (eq type :text) (ereport "0A000" "array_agg currently supports text input"))
       (values argument :text-array))
      ((:sum :avg)
       (unless (member type '(:int2 :int4 :int8 :numeric :float4 :float8 :interval))
         (ereport "42883" "function ~a(~a) does not exist" (second raw) (typed-arg-name argument)))
       (values argument (cond ((eq type :interval) :interval)
                              ((and (eq kind :sum) (member type '(:int2 :int4))) :int8)
                              ((and (eq kind :sum) (member type '(:float4 :float8))) type)
                              ((member type '(:float4 :float8)) :float8)
                              (t :numeric)))))))

(defun analyze-aggregate-call (raw scope)
  ;; Semantic ownership follows referenced levels, not merely the syntactic query.
  (unless *select-aggregate-analysis*
    (ereport "42803" "aggregate functions are not allowed in this expression"))
  (when (and (null *select-analysis-frame*) *inside-aggregate-input-p*)
    (ereport "42803" "aggregate function calls cannot be nested"))
  (let ((kind (raw-aggregate-kind raw))
        (frame *select-analysis-frame*))
    (multiple-value-bind (input type)
        (let ((old (and frame (getf frame :inside-input))))
          (unwind-protect
               (progn
                 (when frame (setf (getf frame :inside-input) t))
                 (analyze-aggregate-input raw scope kind))
            (when frame (setf (getf frame :inside-input) old))))
      (let* ((depth (aggregate-input-owner-depth input))
             (owner (loop with current = frame repeat depth
                          do (setf current (getf current :parent))
                          finally (return current)))
             (analysis (or owner *select-aggregate-analysis*)))
        (when (and owner (not (member (getf owner :kind) '(:select-target :order-by :having))))
          (ereport "42803" "aggregate functions are not allowed in this expression"))
        (when (and owner (getf owner :inside-input))
          (ereport "42803" "aggregate function calls cannot be nested"))
        (when (plusp depth)
          (setf input (rebase-aggregate-input input depth)))
        (let* ((descriptors (getf analysis :descriptors))
               (existing
                 (when (or (eq *aggregate-order-mode* :existing-only)
                           (not (analyzed-expression-volatile-p input)))
                   (position-if (lambda (descriptor)
                                  (and (eq kind (getf descriptor :kind))
                                       (selected-expression-equal-p input (getf descriptor :input))))
                                descriptors)))
               (index (or existing (length descriptors))))
          (when (and (eq *aggregate-order-mode* :existing-only) (null existing))
            (ereport "42803" "aggregate is not present in the SELECT list"))
          (unless existing
            (vector-push-extend (list :kind kind :input input :type type :source-expr raw) descriptors))
          (let ((reference (append (aggregate-output-ref
                                    (+ (length (getf analysis :group-keys)) index 1) type)
                                   (list :aggregate-output t
                                         :aggregate-key
                                         (make-sql-expression-key
                                          :value (list :aggregate kind type
                                                       (subquery-semantic-key input)))))))
            (if (zerop depth) reference
                (list :outer-ref depth reference :type type :typmod -1))))))))

(defun aggregate-expression-signature (form descriptors)
  "Compare semantic aggregate identities independently of their result slots."
  (cond
    ((atom form) form)
    ((eq (first form) :outer-ref) form)
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
    ((member (first form) '(:outer-ref :window-ref)) form)
    ((eq (first form) :subplan)
     (rewrite-subplan-group-columns form group-keys scope))
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
  "Analyze targets with query-owned descriptors, or a fresh standalone aggregate context."
  (let* ((*select-aggregate-analysis*
           (or *select-aggregate-analysis*
               (list :group-keys group-keys
                     :descriptors (make-array 0 :adjustable t :fill-pointer 0))))
         (descriptors (getf *select-aggregate-analysis* :descriptors))
         (targets
           (loop for target in (expand-select-targets raw-targets scope)
                 for before = (length descriptors)
                 for analyzed = (analyze-expanded-target target scope)
                 unless *defer-group-rewrite-p*
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

(defun analyze-order-expression (raw scope targets)
  "Reuse complete selected expressions, discarding only unused probe transitions."
  ;; A probe can add transitions or window descriptors; roll them back only when
  ;; the complete expression matches a target (parse_clause.c SQL99 matching).
  (let* ((key-targets *window-key-targets*)
         (contexts (if *select-analysis-frame*
                       (loop for frame = *select-analysis-frame* then (getf frame :parent)
                             while frame collect frame)
                       (list *select-aggregate-analysis*)))
         (snapshots (loop for context in (append contexts
                                                 (list (list :descriptors *select-window-functions*)
                                                       (list :descriptors *select-window-specifications*)))
                          for descriptors = (getf context :descriptors)
                          when (and descriptors (array-has-fill-pointer-p descriptors))
                            collect (cons descriptors (fill-pointer descriptors))))
         (expression (analyze-expr raw scope))
         (position (position expression targets
                             :key (lambda (target) (getf target :expr))
                             :test #'selected-expression-equal-p)))
    (if position
        (progn
          (setf *window-key-targets* key-targets)
          (dolist (snapshot snapshots)
            (setf (fill-pointer (car snapshot)) (cdr snapshot)))
          (getf (nth position targets) :expr))
        expression)))

(defun analyze-aggregate-order (order scope targets group-keys &optional descriptors allow-new)
  "Resolve selected and earlier hidden ORDER targets before adding transitions."
  ;; Deduplicate canonical EQ expressions, preserving distinct alias/ordinal slots.
  (let ((*select-set-functions-allowed-p* t)
        (*select-aggregate-analysis*
          (if *defer-group-rewrite-p* *select-analysis-frame*
              (list :group-keys group-keys :descriptors
                    (if (vectorp descriptors) descriptors (coerce descriptors 'vector)))))
        (*aggregate-order-mode* (if allow-new :allow-new :existing-only))
        (seen nil)
        (result nil))
    (dolist (item order (nreverse result))
      (let* ((expression
               (multiple-value-bind (position selected-p)
                   (selected-order-target-position item targets)
                 (if selected-p (getf (nth position targets) :expr)
                     (analyze-order-expression (getf item :expr) scope
                                               (append targets seen)))))
             (duplicate (find expression seen :key (lambda (target) (getf target :expr))
                                              :test #'eq)))
        (unless duplicate
          (push (list :expr expression) seen)
          (push (cons (if (or *defer-group-rewrite-p*
                              (nth-value 1 (selected-order-target-position item targets)))
                          expression
                          (rewrite-aggregate-columns expression group-keys scope))
                      (getf item :desc)) result))))))

(defun analyze-order (order table targets)
  ;; Earlier hidden ORDER targets also participate in SQL99 expression matching.
  (let ((*select-set-functions-allowed-p* t)
        (seen nil)
        (result nil))
    (dolist (item order (nreverse result))
      (let* ((expression
               (multiple-value-bind (position selected-p)
                   (selected-order-target-position item targets)
                 (if selected-p (getf (nth position targets) :expr)
                     (analyze-order-expression (getf item :expr) table
                                               (append targets seen)))))
             (duplicate (find expression seen :key (lambda (target) (getf target :expr))
                                              :test #'eq)))
        (unless duplicate
          (push (list :expr expression) seen)
          (push (cons expression (getf item :desc)) result))))))

(defun sql-type-category (type)
  (cond ((member type '(:int2 :int4 :int8 :oid :numeric :float4 :float8)) :numeric)
        ((member type '(:date :time :timetz :timestamp :timestamptz)) :datetime)
        ((member type '(:text :varchar :name)) :string)
        ((or (eq type :int2vector) (sql-array-element-type type)) :array)
        ((member type '(:char :pg-node-tree)) :internal)
        (t type)))

(defun sql-implicit-coercion-p (source target)
  "The supported implicit pg_cast edges; explicit I/O casts are broader."
  (or (eq source target)
      (and (eq source :int2vector) (sql-array-element-type target)
           (sql-implicit-coercion-p :int2 (sql-array-element-type target)))
      (and (sql-array-element-type source) (sql-array-element-type target)
           (sql-implicit-coercion-p (sql-array-element-type source)
                                    (sql-array-element-type target)))
      (member (list source target)
              '((:int2 :int4) (:int2 :int8) (:int2 :oid) (:int4 :int8) (:int4 :oid) (:int8 :oid)
                (:int2 :numeric) (:int4 :numeric) (:int8 :numeric)
                (:int2 :float4) (:int4 :float4) (:int8 :float4) (:numeric :float4)
                (:int2 :float8) (:int4 :float8) (:int8 :float8) (:numeric :float8)
                (:float4 :float8)
                (:date :timestamp) (:date :timestamptz) (:timestamp :timestamptz) (:time :timetz)
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
       (when (member (getf (nth position targets) :type) '(:json :json-array))
         (ereport "42883" "could not identify an ordering operator for type json"))
       (cons (getf (nth position targets) :expr) (getf item :desc))))
   order))

(defun selected-expression-equal-p (left right)
  ;; SQL99 target matching compares meaning, independent of transition slots.
  (cond ((equal left right) t)
        ((and (sql-jsonb-p left) (sql-jsonb-p right))
         (string= (sql-jsonb-output left) (sql-jsonb-output right)))
        ((and (sql-decimal-p left) (sql-decimal-p right))
         (and (= (sql-decimal-coefficient left) (sql-decimal-coefficient right))
              (= (sql-decimal-scale left) (sql-decimal-scale right))))
        ((and (sql-expression-key-p left) (sql-expression-key-p right))
         (equal (sql-expression-key-value left) (sql-expression-key-value right)))
        ((and (consp left) (consp right)
              (member (first left) '(:col :window-ref))
              (member (first right) '(:col :window-ref))
              (getf (cddr left) :window-key) (getf (cddr right) :window-key))
         (equal (sql-expression-key-value (getf (cddr left) :window-key))
                (sql-expression-key-value (getf (cddr right) :window-key))))
        ((and (consp left) (consp right)
              (eq (first left) :col) (eq (first right) :col)
              (getf (cddr left) :aggregate-key) (getf (cddr right) :aggregate-key))
         (equal (sql-expression-key-value (getf (cddr left) :aggregate-key))
                (sql-expression-key-value (getf (cddr right) :aggregate-key))))
        ((and (subquery-expression-p left) (subquery-expression-p right))
         (and (eq (expr-type left) (expr-type right))
              (eql (expr-typmod left) (expr-typmod right))
              (equal (subquery-semantic-key (second left))
                     (subquery-semantic-key (second right)))))
        ((and (consp left) (consp right))
         (or (same-group-column-p left right)
             (and (selected-expression-equal-p (car left) (car right))
                  (selected-expression-equal-p (cdr left) (cdr right)))))
        (t nil)))

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
                       (analyze-order-expression (getf item :expr) scope targets)))
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
    ((and (sql-jsonb-p a) (sql-jsonb-p b)) (sql-jsonb-compare a b))
    ((and (member a '(t nil)) (member b '(t nil)))
     (cond ((eq a b) 0) (a 1) (t -1)))
    ((and (sql-numeric-p a) (sql-numeric-p b))
     (sql-numeric-compare a b))
    ((and (sql-timetz-p a) (sql-timetz-p b)) (sql-timetz-compare a b))
    ((and (sql-interval-p a) (sql-interval-p b))
     (let ((left (sql-interval-comparison-value a)) (right (sql-interval-comparison-value b)))
       (cond ((< left right) -1) ((> left right) 1) (t 0))))
    ((and (floatp a) (floatp b)) (sql-float-compare a b))
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
    ((and (sql-array-p a) (vectorp b) (not (stringp b)))
     (compare-3way a (make-sql-array :element-type-id :int2 :elements b
                                     :dimensions (list (length b)) :lower-bounds '(0))))
    ((and (vectorp a) (not (stringp a)) (sql-array-p b))
     (compare-3way (make-sql-array :element-type-id :int2 :elements a
                                   :dimensions (list (length a)) :lower-bounds '(0)) b))
    ((and (sql-array-p a) (sql-array-p b))
     (when (or (eq (sql-array-element-type-id a) :json) (eq (sql-array-element-type-id b) :json))
       (ereport "42883" "could not identify a comparison function for type json"))
     (let ((comparison (compare-sql-array-values (sql-array-elements a) (sql-array-elements b))))
       (if (zerop comparison)
           (let ((rank-comparison (- (length (sql-array-dimensions a)) (length (sql-array-dimensions b)))))
             (if (zerop rank-comparison)
                 (let ((dimensions (compare-integer-vectors (coerce (sql-array-dimensions a) 'vector)
                                                            (coerce (sql-array-dimensions b) 'vector))))
                   (if (zerop dimensions)
                       (compare-integer-vectors (coerce (sql-array-lower-bounds a) 'vector)
                                                (coerce (sql-array-lower-bounds b) 'vector))
                       dimensions))
                 rank-comparison))
           comparison)))
    ((and (vectorp a) (vectorp b)) (compare-sql-array-values a b))
    (t
     (ereport "42883" "operator does not exist: ~a and ~a"
              (type-of a) (type-of b)))))

(defun sql-compare (op a b)
  (sql-strict-let ((left a) (right b))
    (when (and (member op '("=" "<>") :test #'string=)
               (sql-array-p left) (sql-array-p right)
               (or (eq (sql-array-element-type-id left) :json)
                   (eq (sql-array-element-type-id right) :json))
               (or (not (equal (sql-array-dimensions left) (sql-array-dimensions right)))
                   (not (equal (sql-array-lower-bounds left) (sql-array-lower-bounds right)))))
      (return-from sql-compare (string= op "<>")))
    (when (and (member op '("=" "<>") :test #'string=)
               (sql-array-p left) (sql-array-p right)
               (or (eq (sql-array-element-type-id left) :json)
                   (eq (sql-array-element-type-id right) :json)))
      (ereport "42883" "could not identify an equality operator for type json"))
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
       (rem left right))
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

(defun eval-recovery-wal-function (name)
  "Report actual recovery state; a base image has no walreceiver position."
  (cond
    ((string= name "pg_is_in_recovery") (recovery-in-progress-p))
    ((string= name "pg_last_wal_receive_lsn")
     (or (getf *physical-recovery-state* :receive-lsn) +null+))
    ((string= name "pg_last_wal_replay_lsn") (or *last-replay-lsn* +null+))
    (t
     (prevent-recovery-primary-operation name)
     (if (string= name "pg_current_wal_flush_lsn") *wal-durable-lsn* *wal-end-lsn*))))

(defun eval-logical-helper-function (name args)
  "Evaluate a supported logical helper, taking the write lock for catalog reads."
  (cond
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
    (t (ereport "42883" "function ~a(...) does not exist" name))))

(defun sql-integer-gcd-lcm (left right type lcm-p)
  "Compute integer GCD or LCM, checking the result against TYPE's range."
  (let* ((divisor (gcd left right))
         (result (if lcm-p
                     (if (zerop divisor) 0 (abs (* (truncate left divisor) right)))
                     divisor))
         (maximum (if (eq type :int4) +int4-max+ +int8-max+)))
    (when (> result maximum)
      (ereport "22003" "~a out of range" (if (eq type :int4) "integer" "bigint")))
    result))

(defun sql-factorial (number)
  "Compute PostgreSQL's numeric factorial, checking for interrupts."
  (when (minusp number) (ereport "22003" "factorial of a negative number is undefined"))
  (when (> number 32177) (ereport "22003" "value overflows numeric format"))
  (let ((result (if (zerop number) 1 number)))
    ;; Like numeric_fac, check before each multiplication; 0, 1 and 2
    ;; have no loop iterations and therefore no loop interrupt check.
    (loop for factor downfrom (1- number) above 1
          do (check-for-interrupts)
             (setf result (* result factor)))
    (sql-numeric-result result 0)))

(defun eval-numeric-arithmetic-function (name args type)
  "Evaluate strict arithmetic calls on already evaluated, coerced arguments.
DIV truncates toward zero, integer MOD uses integer remainder, and the numeric
aliases share the corresponding numeric operator implementation."
  (when (member +null+ args) (return-from eval-numeric-arithmetic-function +null+))
  (let ((left (first args)) (right (second args)))
    (cond ((string= name "factorial") (sql-factorial left))
          ((member name '("gcd" "lcm") :test #'string=)
           (if (eq type :numeric)
               (sql-numeric-gcd-lcm left right (string= name "lcm"))
               (sql-integer-gcd-lcm left right type (string= name "lcm"))))
          ((member name '("div" "numeric_div_trunc") :test #'string=)
           (sql-numeric-div-trunc left right))
          ((and (string= name "mod") (member type '(:int2 :int4 :int8)))
           (sql-arith "%" left right))
          (t (let ((operator (cond ((string= name "numeric_add") "+")
                                   ((string= name "numeric_sub") "-")
                                   ((string= name "numeric_mul") "*")
                                   ((string= name "numeric_div") "/")
                                   (t "%"))))
               (sql-numeric-arithmetic operator left right))))))

(defun eval-func (name args)
  (cond
    ((physical-slot-function-signature name) (eval-physical-slot-function name args))
    ((string= name "width_bucket") (sql-width-bucket args))
    ((and (member name '("nextval" "currval" "setval") :test #'string=)
          (member +null+ args))
     +null+)
    ((member name '("abs" "numeric_abs" "sign" "round" "trunc" "ceil" "ceiling" "floor")
             :test #'string=)
     (if (member +null+ args) +null+
         (sql-numeric-basic-function name (first args) (if (rest args) (second args) 0))))
    ((string= name "pg_promote") (pg-promote (first args) (second args)))
    ((string= name "pg_stat_get_wal_receiver") (wal-receiver-result))
    ((string= name "pg_postmaster_start_time") (postmaster-start-timestamp))
    ((member name '("json_typeof" "jsonb_typeof" "json_array_length" "jsonb_array_length")
             :test #'string=)
     (sql-strict-let ((value (first args)))
       (cond ((string= name "json_typeof") (sql-json-source-kind value))
             ((string= name "jsonb_typeof") (sql-jsonb-kind value))
             ((string= name "json_array_length") (sql-json-source-array-length value))
             (t (sql-jsonb-array-length value)))))
    ((member name '("substr" "substring") :test #'string=) (eval-substr-function args))
    ((string= name "pg_wal_lsn_diff") (eval-wal-lsn-diff args))
    ((string= name "pg_walfile_name") (eval-wal-filename args))
    ((member name '("lower" "current_setting") :test #'string=)
     (eval-setting-function name args))
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
    ((member name '("pg_current_wal_lsn" "pg_current_wal_flush_lsn" "pg_is_in_recovery"
                    "pg_last_wal_receive_lsn" "pg_last_wal_replay_lsn") :test #'string=)
     (eval-recovery-wal-function name))
    ((member name '("pg_wal_replay_pause" "pg_wal_replay_resume"
                    "pg_is_wal_replay_paused" "pg_get_wal_replay_pause_state") :test #'string=)
     (physical-replay-pause-command name))
    ((string= name "pg_lisp_cluster_identity")
     (catalog-cluster-identity *catalog*))
    ((string= name "pg_export_snapshot") (export-transaction-snapshot))
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
    ((logical-helper-function-p name)
     (eval-logical-helper-function name args))
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
    ((member name '("length" "char_length" "character_length" "octet_length")
             :test #'string=)
     (sql-strict-let ((value (first args)))
       (if (and (string= name "octet_length") (stringp value))
           (length (string->bytes value))
           (length value))))
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

(defun eval-coalesce-expression (expression environment)
  "Evaluate left to right and stop at the first SQL non-NULL, including NIL or zero."
  (dolist (argument (second expression) +null+)
    (let ((value (eval-expr argument environment)))
      (unless (eq value +null+) (return value)))))

(defun eval-case-expression (expression environment)
  (dolist (branch (second expression))
    (when (eq (eval-expr (first branch) environment) t)
      (return-from eval-case-expression (eval-expr (second branch) environment))))
  (eval-expr (third expression) environment))

(defun eval-any-expression (expression environment)
  "Match C ScalarArrayOp's ANY/ALL identity, short circuit and NULL semantics."
  (let ((left (eval-expr (third expression) environment))
        (array (eval-expr (fourth expression) environment))
        (all-p (eq (first expression) :all))
        (null-seen nil))
    (when (eq array +null+) (return-from eval-any-expression +null+))
    (when (sql-array-p array) (setf array (sql-array-elements array)))
    ;; Convert the complete array before comparison, as C's array input does:
    ;; a decisive first element cannot hide an invalid later input value.
    (setf array (map 'vector
                     (lambda (element)
                       (sql-cast-value element (getf (nthcdr 4 expression) :element-source)
                                       (getf (nthcdr 4 expression) :element-target) -1))
                     array))
    (loop for value across array
          for result = (sql-compare (second expression) left value)
          do (when (eq result (not all-p)) (return-from eval-any-expression result))
             (when (eq result +null+) (setf null-seen t)))
    (if null-seen +null+ all-p)))

(defun eval-array-constructor (expression environment)
  (let* ((values (map 'vector (lambda (argument) (eval-expr argument environment))
                      (second expression)))
         (element-type (sql-array-element-type (expr-type expression))))
    (if (getf (cddr expression) :multidims)
        (let ((shape nil) (bounds nil) (nonempty nil) (empty nil))
          (loop for value across values do
            (when (and (not nonempty) (sql-array-p value)
                       (>= (length (sql-array-dimensions value)) 6))
              (ereport "54000" "number of array dimensions exceeds the maximum allowed"))
            (if (or (eq value +null+) (null (sql-array-dimensions value)))
                (setf empty t)
                (if nonempty
                    (unless (and (equal shape (sql-array-dimensions value))
                                 (equal bounds (sql-array-lower-bounds value)))
                      (ereport "2202E" "multidimensional arrays must have array expressions with matching dimensions"))
                    (setf nonempty t shape (sql-array-dimensions value)
                          bounds (sql-array-lower-bounds value)))))
          (when (and empty nonempty)
            (ereport "2202E" "multidimensional arrays must have array expressions with matching dimensions"))
          (if nonempty
              (make-sql-array :element-type-id element-type
                              :elements (apply #'concatenate 'vector (map 'list #'sql-array-elements values))
                              :dimensions (cons (length values) shape) :lower-bounds (cons 1 bounds))
              (make-sql-array :element-type-id element-type :elements #() :dimensions nil :lower-bounds nil)))
        (make-sql-array :element-type-id element-type :elements values
                        :dimensions (and (plusp (length values)) (list (length values)))
                        :lower-bounds (and (plusp (length values)) (list 1))))))

(defun eval-array-length (expression environment)
  (sql-strict-let ((array (eval-expr (second expression) environment))
                   (dimension (eval-expr (third expression) environment)))
    (if (sql-array-p array)
        (if (<= 1 dimension (length (sql-array-dimensions array)))
            (nth (1- dimension) (sql-array-dimensions array)) +null+)
        (if (= dimension 1) (length array) +null+))))

(defun sql-random-numeric (minimum maximum)
  (when (sql-numeric-special-p minimum)
    (ereport "22023" "lower bound cannot be ~a" (if (eq minimum :numeric-nan) "NaN" "infinity")))
  (when (sql-numeric-special-p maximum)
    (ereport "22023" "upper bound cannot be ~a" (if (eq maximum :numeric-nan) "NaN" "infinity")))
  (let* ((scale (max (sql-numeric-scale minimum) (sql-numeric-scale maximum)))
         (factor (expt 10 scale))
         (low (* factor (sql-numeric-rational minimum)))
         (high (* factor (sql-numeric-rational maximum)))
         (range (- high low)))
    (when (minusp range) (ereport "22023" "lower bound must be less than or equal to upper bound"))
    (when (zerop range) (return-from sql-random-numeric (make-sql-decimal low scale)))
    (let* ((padding (expt 10 (mod (- scale) 4)))
           (limit (* range padding))
           (digits (loop for n = limit then (floor n 10000) count t while (>= n 10000)))
           (prefix-count (min digits 4))
           (suffix-count (- digits prefix-count))
           (prefix (floor limit (expt 10000 suffix-count)))
           (whole (- digits (if (= padding 1) 0 1))))
      (loop
        for result = (if (and (zerop suffix-count) (/= padding 1))
                         (* padding (sql-prng-range 0 (floor prefix padding)))
                         (sql-prng-range 0 prefix))
        do (let ((i prefix-count))
             (loop while (< i (- whole 3))
                   do (let ((v (sql-prng-range 0 (1- (expt 10000 4)))))
                        (dotimes (j 4)
                          (multiple-value-bind (q digit) (floor v 10000)
                            (setf result (+ (* result 10000) digit) v q))))
                      (incf i 4))
             (loop while (< i whole) do
               (setf result (+ (* result 10000) (sql-prng-range 0 9999))) (incf i))
             (when (< i digits)
               (setf result (+ (* result 10000) (* padding (sql-prng-range 0 (1- (/ 10000 padding))))))))
        when (<= result limit) return (make-sql-decimal (+ low (/ result padding)) scale)))))

(defun eval-random-function (name arguments type)
  (when (member +null+ arguments) (return-from eval-random-function +null+))
  (cond
    ((string= name "setseed")
     (let ((seed (first arguments)))
       (unless (and (not (sb-ext:float-nan-p seed)) (<= -1d0 seed) (<= seed 1d0))
         (ereport "22023" "setseed parameter ~a is out of allowed range [-1,1]"
                  (sql-float-output seed :float8 -9)))
       (sql-prng-seed (truncate (* (1- (ash 1 52)) seed)))
       nil))
    ((null arguments) (scale-float (float (ash (sql-prng-next) -12) 1d0) -52))
    ((eq type :numeric) (sql-random-numeric (first arguments) (second arguments)))
    (t (when (> (first arguments) (second arguments))
         (ereport "22023" "lower bound must be less than or equal to upper bound"))
       (sql-prng-range (first arguments) (second arguments)))))

(defun eval-analyzed-function (name arguments type)
  "Evaluate a resolved call using its coerced argument values and result type.
Arguments have already been evaluated in order, including for strict calls.
Map float catalog names and dispatch integer catalog names only here, after
expression identity has been resolved."
  (setf name (or (second (float-catalog-alias name)) name))
  (cond
    ((member name '("jsonb_set" "jsonb_insert") :test #'string=)
     (if (member +null+ arguments) +null+
         (if (string= name "jsonb_insert")
             (sql-jsonb-set (first arguments) (second arguments) (third arguments) t
                            :insert-p t :after-p (and (= (length arguments) 4) (fourth arguments)))
             (sql-jsonb-set (first arguments) (second arguments) (third arguments)
                            (if (fourth arguments) (fourth arguments) (= (length arguments) 3))))))
    ((integer-catalog-alias-p name)
     (if (member +null+ arguments) +null+
         (let ((suffix (subseq name 4)))
           (cond ((string= suffix "abs") (eval-analyzed-function "abs" arguments type))
                 ((string= suffix "not") (lognot (first arguments)))
                 ;; Share remainder and zero-divisor semantics after preserving call identity.
                 ((string= suffix "mod") (sql-arith "%" (first arguments) (second arguments)))
                 (t (sql-integer-bitwise
                     (cdr (assoc suffix '(("and" . "&") ("or" . "|") ("xor" . "#") ("shl" . "<<") ("shr" . ">>")) :test #'string=))
                     (first arguments) (second arguments) type))))))
    ((member name '("random" "setseed") :test #'string=)
     (eval-random-function name arguments type))
    ((and (member name '("abs" "numeric_abs" "sign" "round" "trunc" "ceil" "ceiling" "floor")
                  :test #'string=)
          (member +null+ arguments))
     +null+)
    ((and (member name '("abs" "numeric_abs" "sign" "round" "trunc" "ceil" "ceiling" "floor")
                  :test #'string=)
          (member type '(:float4 :float8)))
     (sql-float-basic-function name (first arguments)))
    ((member name '("sqrt" "numeric_sqrt") :test #'string=)
     (let ((value (first arguments)))
       (cond ((eq value +null+) +null+)
             ((eq type :numeric) (sql-numeric-sqrt value))
             (t (sql-float-sqrt value)))))
    ((trig-function-p name)
     (cond ((string= name "pi") pi)
           ((member +null+ arguments) +null+)
           (t (sql-float-trig name arguments))))
    ((numeric-helper-function-p name) (eval-numeric-helper-function name arguments))
    ((numeric-arithmetic-function-p name)
     (eval-numeric-arithmetic-function name arguments type))
    ((member name '("scale" "min_scale" "trim_scale") :test #'string=)
     (let ((value (first arguments)))
       (cond ((eq value +null+) +null+)
             ((string= name "trim_scale") (sql-numeric-trim-scale value))
             ((sql-numeric-special-p value) +null+)
             ((string= name "scale") (sql-numeric-scale value))
             (t (sql-numeric-min-scale value)))))
    ((member name '("power" "pow" "numeric_power" "dpow") :test #'string=)
     (cond ((member +null+ arguments) +null+)
           ((eq type :numeric) (sql-numeric-power (first arguments) (second arguments)))
           (t (sql-float-power (first arguments) (second arguments)))))
    ((member name '("log" "log10" "numeric_log") :test #'string=)
     (cond ((member +null+ arguments) +null+)
           ((eq type :numeric)
            (if (rest arguments)
                (sql-numeric-log (first arguments) (second arguments))
                (sql-numeric-log 10 (first arguments))))
           (t (sql-float-log10 (first arguments)))))
    ((member name '("ln" "numeric_ln") :test #'string=)
     (let ((value (first arguments)))
       (cond ((eq value +null+) +null+)
             ((eq type :numeric) (sql-numeric-ln value))
             (t (sql-float-ln value)))))
    ((member name '("exp" "numeric_exp") :test #'string=)
     (let ((value (first arguments)))
       (cond ((eq value +null+) +null+)
             ((eq type :numeric) (sql-numeric-exp value))
             (t (sql-float-exp value)))))
    ((and (string= name "abs") (member type '(:int2 :int4 :int8)))
     (let ((value (abs (first arguments))))
       (when (and (eq type :int8) (> value +int8-max+))
         (ereport "22003" "bigint out of range"))
       (sql-cast-value value type type -1)))
    (t (eval-func name arguments))))

(defun sql-integer-bitwise (op l r type)
  "Evaluate bitwise operators on already coerced, non-NULL integer operands.
C's signed shifts can be undefined or implementation-defined. Match the x86
behavior used by the C peer: mask the count, promote int2 to 32 bits, then
truncate and interpret the result at TYPE's signed width."
  (let* ((bits (ecase type (:int2 16) (:int4 32) (:int8 64)))
         (shift-bits (if (= bits 16) 32 bits))
         (raw (cond ((string= op "&") (logand l r))
                    ((string= op "|") (logior l r))
                    ((string= op "#") (logxor l r))
                    ((string= op "<<") (ash l (mod r shift-bits)))
                    (t (ash l (- (mod r shift-bits))))))
         (unsigned (ldb (byte bits 0) raw)))
    (if (logbitp (1- bits) unsigned) (- unsigned (ash 1 bits)) unsigned)))

(declaim (ftype function sql-to-jsonb))

(defun eval-expr (expr env)
  (case (first expr)
    (:parameter (query-parameter-slot (second expr) *query-parameter-values*))
    (:subplan (eval-subquery-expression (second expr) env))
    (:assignment (eval-subquery-assignment expr env))
    (:subquery-output *subquery-output-value*)
    (:outer-ref
     (let* ((depth (second expr))
            (frame (nth (1- depth) *subquery-outer-frames*))
            (*subquery-outer-frames* (nthcdr depth *subquery-outer-frames*)))
       (unless frame (error "Missing outer query parameter frame"))
       (eval-expr (third expr) (car frame))))
    ((:const :scalar-array-const) (second expr))
    (:coalesce (eval-coalesce-expression expr env))
    (:case (eval-case-expression expr env))
    ((:any :all) (eval-any-expression expr env))
    (:array-length (eval-array-length expr env))
    (:cast (sql-cast-value (eval-expr (second expr) env)
                           (expr-type (second expr)) (expr-type expr) (expr-typmod expr)))
    (:bool (second expr))
    (:array (eval-array-constructor expr env))
    (:array-subscript (eval-array-subscript expr env))
    (:array-slice (eval-array-slice expr env))
    (:array-assign (eval-array-assignment expr env))
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
    (:concat
     (sql-strict-let ((left (eval-expr (second expr) env))
                      (right (eval-expr (third expr) env)))
       (case (expr-type expr)
         (:bytea (sql-bytea-concat left right))
         (:jsonb (sql-jsonb-concat left right))
         (otherwise (concatenate 'string left right)))))
    (:binop
     (let ((op (second expr))
           (l (eval-expr (third expr) env))
           (r (eval-expr (fourth expr) env)))
       (if (member op '("=" "<>" "<" "<=" ">" ">=") :test #'string=)
           (sql-compare op l r)
           (let ((value (cond ((or (eq l +null+) (eq r +null+)) +null+)
                              ((member op '("@>" "<@") :test #'string=)
                               (if (string= op "@>") (sql-jsonb-contains-p l r)
                                   (sql-jsonb-contains-p r l)))
                              ((string= op "?") (sql-jsonb-exists-p l r))
                              ((member op '("?|" "?&") :test #'string=)
                               (sql-jsonb-exists-keys-p
                                l (if (sql-array-p r) (sql-array-elements r) r)
                                (string= op "?&")))
                              ((string= op "#-") (sql-jsonb-delete-path l r))
                              ((and (string= op "-") (eq (expr-type (third expr)) :jsonb))
                               (sql-jsonb-delete l r (case (expr-type (fourth expr))
                                                       (:int4 :index) (:text-array :keys) (otherwise :key))))
                              ((member op '("#>" "#>>") :test #'string=)
                               (sql-json-path-extract
                                l (if (sql-array-p r) (sql-array-elements r) r)
                                (eq (expr-type (third expr)) :jsonb)
                                (string= op "#>>")))
                              ((member op '("->" "->>") :test #'string=)
                               (if (eq (expr-type (third expr)) :json)
                                   (sql-json-extract l r (string= op "->>"))
                                   (sql-jsonb-extract l r (string= op "->>"))))
                              ((or (member (expr-type (third expr)) '(:time :timestamp :timestamptz :interval))
                                   (member (expr-type (fourth expr)) '(:time :timestamp :timestamptz :interval)))
                               (sql-temporal-arithmetic op l r (expr-type (third expr)) (expr-type (fourth expr))
                                                        (when (or (eq (expr-type (third expr)) :timestamptz)
                                                                  (eq (expr-type (fourth expr)) :timestamptz))
                                                          (sql-session-timezone))))
                              ((or (eq (expr-type (third expr)) :date) (eq (expr-type (fourth expr)) :date))
                               (sql-date-arithmetic op l r (expr-type (third expr)) (expr-type (fourth expr))))
                              ((member op '("&" "|" "#" "<<" ">>") :test #'string=)
                               (sql-integer-bitwise op l r (expr-type expr)))
                              ((string= op "^")
                               (if (eq (expr-type expr) :numeric)
                                   (sql-numeric-power l r) (sql-float-power l r)))
                              ((eq (expr-type expr) :numeric) (sql-numeric-arithmetic op l r))
                              ((member (expr-type expr) '(:float4 :float8)) (sql-float-arithmetic op l r))
                              (t (sql-arith op l r)))))
             (if (member (expr-type expr) '(:int2 :int4 :int8))
                 (sql-cast-value value (expr-type expr) (expr-type expr) -1) value)))))
    (:unary
     (sql-strict-let ((value (eval-expr (third expr) env)))
       (cond
         ((string= (second expr) "~") (lognot value))
         ((member (second expr) '("@" "|/" "||/") :test #'string=)
          (eval-analyzed-function (cond ((string= (second expr) "@") "abs")
                                        ((string= (second expr) "|/") "sqrt")
                                        (t "cbrt"))
                                  (list value) (expr-type expr)))
         ((string= (second expr) "-")
          (let ((result (cond ((sql-interval-p value) (sql-interval-add (make-sql-interval 0 0 0) value t))
                              ((sql-numeric-p value) (sql-numeric-negate value))
                              (t (- value)))))
            (if (member (expr-type expr) '(:int2 :int4 :int8))
                (sql-cast-value result (expr-type expr) (expr-type expr) -1) result)))
         (t (ereport "42883" "unsupported unary operator ~a"
                     (second expr))))))
    (:and
     (let ((left (eval-expr (second expr) env)))
       (if (null left) nil
           (sql-3val-and left (eval-expr (third expr) env)))))
    (:or
     (let ((left (eval-expr (second expr) env)))
       (if (eq left t) t
           (sql-3val-or left (eval-expr (third expr) env)))))
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
     (let ((slice (assoc expr *toast-slice-current* :test #'eq)))
       (when (and slice (eq env (fourth (cdr slice))))
         (return-from eval-expr
           (if (member (second expr) '("length" "octet_length" "char_length" "character_length") :test #'string=)
               (eval-toast-length expr (cdr slice))
               (eval-toast-substring (third expr) (cdr slice) (expr-type expr))))))
     (let* ((name (second expr))
            (arguments (mapcar (lambda (a) (eval-expr a env)) (third expr)))
            (type (expr-type expr)))
       (if (string= name "to_jsonb")
           (sql-to-jsonb (first arguments) (expr-type (first (third expr))))
           (eval-analyzed-function name arguments type))))
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

(defvar *sql-array-output-max-bytes* (- #x3fffffff 4)
  "Maximum UTF-8 bytes in an array's textual output.")

(defclass sql-array-jsonb-count-stream (logical-utf8-output-stream)
  ((limit :initarg :limit :reader sql-array-jsonb-count-limit)))

(defmethod sb-gray:stream-write-char ((stream sql-array-jsonb-count-stream) character)
  (prog1 (call-next-method)
    (when (zerop (length (logical-utf8-characters stream)))
      (check-for-interrupts)
      (when (> (logical-utf8-size stream) (sql-array-jsonb-count-limit stream))
        (ereport "54000" "array output exceeds maximum size")))))

(defun sql-array-output (array)
  "Render an array while enforcing the PostgreSQL varlena output bound."
  (let ((elements (sql-array-elements array))
        (element-type (sql-array-element-type-id array))
        (dimensions (sql-array-dimensions array))
        (bounds (sql-array-lower-bounds array))
        (position 0)
        (used 0))
    (with-output-to-string (stream)
      (labels ((reserve (bytes)
                 (incf used bytes)
                 (when (> used *sql-array-output-max-bytes*)
                   (ereport "54000" "array output exceeds maximum size")))
               (emit-ascii (text)
                 (reserve (length text))
                 (write-string text stream))
               (element (value)
                 (if (eq value +null+)
                     (emit-ascii "NULL")
                     (progn
                       ;; JSONB numeric text can be much larger than its compact storage.
                       ;; Count it before materializing an individual element.
                       (when (eq element-type :jsonb)
                         (let ((counter (make-instance 'sql-array-jsonb-count-stream :buffer nil
                                                                                     :limit (- *sql-array-output-max-bytes* used))))
                           (sql-jsonb-write value counter)
                           (logical-flush-utf8-output counter)
                           (when (> (+ used (logical-utf8-size counter)) *sql-array-output-max-bytes*)
                             (ereport "54000" "array output exceeds maximum size"))))
                       (when (eq element-type :bytea)
                         (let ((format (or (session-extra-setting-value "bytea_output") "hex")))
                           (let ((size (if (string= format "escape")
                                           (loop for byte across value for index from 0
                                                 do (when (zerop (mod index 4096)) (check-for-interrupts))
                                                 sum (cond ((= byte 92) 2)
                                                           ((or (< byte 32) (> byte 126)) 4)
                                                           (t 1)))
                                           (+ 2 (* 2 (length value))))))
                             (when (> (+ used size) *sql-array-output-max-bytes*)
                               (ereport "54000" "array output exceeds maximum size")))))
                       (let* ((text (value->text value element-type))
                              (quote-p (or (zerop (length text)) (string-equal text "NULL")))
                              (escapes 0))
                         (loop for character across text for index from 0
                               do (when (zerop (mod index 4096)) (check-for-interrupts))
                                  (when (or (find character "{},")
                                            (member (char-code character) '(9 10 11 12 13 32 34 92)))
                                    (setf quote-p t))
                                  (when (member (char-code character) '(34 92)) (incf escapes)))
                         (reserve (+ (pgoutput-string-utf8-size text) escapes (if quote-p 2 0)))
                         (when quote-p (write-char (code-char 34) stream))
                         (loop for character across text for index from 0
                               do (when (zerop (mod index 4096)) (check-for-interrupts))
                                  (when (member (char-code character) '(34 92))
                                    (write-char (code-char 92) stream))
                                  (write-char character stream))
                         (when quote-p (write-char (code-char 34) stream))))))
               (emit-level (shape)
                 (emit-ascii "{")
                 (when shape
                   (dotimes (index (first shape))
                     (when (zerop (mod index 4096)) (check-for-interrupts))
                     (when (plusp index) (emit-ascii ","))
                     (if (rest shape)
                         (emit-level (rest shape))
                         (progn (element (aref elements position)) (incf position)))))
                 (emit-ascii "}")))
        (when (and (plusp (length elements)) (some (lambda (bound) (/= bound 1)) bounds))
          (loop for bound in bounds for dimension in dimensions
                do (emit-ascii (format nil "[~d:~d]" bound (+ bound dimension -1))))
          (emit-ascii "="))
        (emit-level dimensions)))))

(defun sql-timestamptz-output (timestamp)
  "Format an instant in the current session time zone."
  (sql-timestamp-output timestamp t (sql-session-timezone)
                        (sql-session-date-style) (sql-session-date-order)))

(defun value->text (v type)
  (cond
    ((eq v +null+) nil)
    ((eq type :void) "")
    ((eq type :bytea)
     (sql-bytea-output v (or (session-extra-setting-value "bytea_output") "hex")))
    ((eq type :jsonb) (sql-jsonb-output v))
    ((member type '(:float4 :float8))
     (sql-float-output v type (if *backend* (parse-integer (session-extra-setting-value "extra_float_digits")) 1)))
    ((eq type :bool) (if v "t" "f"))
    ((eq type :date) (sql-date-output v (sql-session-date-style) (sql-session-date-order)))
    ((eq type :time) (sql-time-output v))
    ((eq type :timetz) (sql-timetz-output v))
    ((eq type :interval) (sql-interval-output v))
    ((eq type :timestamp) (sql-timestamp-output v nil nil (sql-session-date-style) (sql-session-date-order)))
    ((eq type :pg-lsn) (wal-format-lsn v))
    ((eq type :timestamptz) (sql-timestamptz-output v))
    ((eq type :record) (sql-record-output v))
    ((sql-array-element-type type) (sql-array-output v))
    ((eq type :char) (sql-char-output v))
    ((eq type :int2vector)
     (when (and (sql-array-p v)
                (or (some (lambda (x) (eq x +null+)) (sql-array-elements v))
                    (/= (length (sql-array-dimensions v)) 1)))
       (ereport "42804" "array is not a valid int2vector"))
     (format nil "~{~d~^ ~}" (coerce (if (sql-array-p v) (sql-array-elements v) v) 'list)))
    ((or (sql-decimal-p v) (and (eq type :numeric) (sql-numeric-special-p v)))
     (sql-numeric-output v))
    ((stringp v) v)
    (t (princ-to-string v))))

(defun sql-jsonb-timestamp-output (value with-zone)
  "Format JSON's XSD timestamp with the session timezone and fixed ISO style."
  (let* ((text (sql-timestamp-output value with-zone (when with-zone (sql-session-timezone)) :iso))
         (separator (position #\Space text)))
    (when separator (setf (char text separator) #\T))
    (when (and with-zone separator)
      (let* ((bc-p (and (>= (length text) 3) (string= " BC" text :start2 (- (length text) 3))))
             (end (if bc-p (- (length text) 3) (length text)))
             (zone (position-if (lambda (char) (find char "+-")) text :start (1+ separator) :end end)))
        (when (and zone (= (- end zone) 3))
          (setf text (concatenate 'string (subseq text 0 end) ":00" (subseq text end))))))
    text))

(defun sql-to-jsonb (value type)
  "Convert a supported typed SQL value to JSONB without mutating its input."
  (when (eq value +null+) (return-from sql-to-jsonb +null+))
  (let ((element-type (sql-array-element-type type)))
    (labels ((scalar (item scalar-type)
               (cond
                 ((eq item +null+) :json-null)
                 ((eq scalar-type :bool) (if item :json-true :json-false))
                 ((member scalar-type '(:int2 :int4 :int8)) item)
                 ((eq scalar-type :numeric)
                  (if (sql-numeric-special-p item) (sql-numeric-output item) item))
                 ((member scalar-type '(:float4 :float8))
                  ;; datum_to_jsonb_internal routes float output through numeric input.
                  (let ((text (value->text item scalar-type)))
                    (if (or (sb-ext:float-nan-p item) (sb-ext:float-infinity-p item))
                        text (sql-numeric-input text))))
                 ((member scalar-type '(:text :varchar :name)) (copy-seq item))
                 ((eq scalar-type :date) (sql-date-output item :iso))
                 ((eq scalar-type :timestamp) (sql-jsonb-timestamp-output item nil))
                 ((eq scalar-type :timestamptz) (sql-jsonb-timestamp-output item t))
                 ((eq scalar-type :int2vector)
                  (if (sql-array-p item)
                      (array-value item :int2)
                      (map 'vector (lambda (number) (scalar number :int2)) item)))
                 ((member scalar-type '(:bytea :oid :char :bit :time :timetz :interval :pg-lsn))
                  (value->text item scalar-type))
                 ((eq scalar-type :json) (sql-jsonb-value (sql-jsonb-input item)))
                 ((eq scalar-type :jsonb) (sql-jsonb-value item))
                 (t (ereport "0A000" "to_jsonb for type ~a is not supported" scalar-type))))
             (array-value (array item-type)
               (let ((elements (sql-array-elements array)) (position 0))
                 (labels ((dimension (dimensions depth)
                            (when (> depth 512) (ereport "54001" "JSON nesting exceeds supported depth"))
                            (let ((result (make-array (first dimensions))))
                              (dotimes (i (length result) result)
                                (when (zerop (mod position 4096)) (check-for-interrupts))
                                (setf (aref result i)
                                      (if (rest dimensions)
                                          (dimension (rest dimensions) (1+ depth))
                                          (prog1 (scalar (aref elements position) item-type)
                                            (incf position))))))))
                   (if (null (sql-array-dimensions array)) #()
                       (dimension (sql-array-dimensions array) 0))))))
      (let* ((root (if element-type (array-value value element-type) (scalar value type)))
             (pending (list (cons root 0)))
             (visited 0))
        (loop while pending for (item . depth) = (pop pending)
              do (when (> depth 512) (ereport "54001" "JSON nesting exceeds supported depth"))
                 (when (zerop (mod (incf visited) 4096)) (check-for-interrupts))
                 (cond ((jsonb-object-value-p item)
                        (dolist (pair (cdr item)) (push (cons (cdr pair) (1+ depth)) pending)))
                       ((jsonb-array-value-p item)
                        (loop for child across item do (push (cons child (1+ depth)) pending)))))
        (make-sql-jsonb root)))))
