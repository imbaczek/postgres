;;;; query-identity.lisp -- versioned identities of resolved query shapes.

(in-package #:pg)

(defconstant +statement-fingerprint-version+ 1)

(defun statement-array-constant-p (expression)
  "Recognize the literal and external parameter nodes eligible for list squashing."
  (case (first expression)
    ((:const :bool :parameter) t)
    (:cast (statement-array-constant-p (second expression)))))

(defun statement-expression-shape (expression)
  "Build a literal-independent shape or bail out on an unsupported analyzed node."
  (labels ((shape (node)
             (when node
               (let ((type (expr-type node))
                     (typmod (expr-typmod node)))
                 (cons (first node)
                       (cons type
                             (cons typmod (operands node)))))))
           (array-items (items)
             (if (and (rest items) (every #'statement-array-constant-p items))
                 (list :constant-list (expr-type (first items)))
                 (mapcar #'shape items)))
           (operands (node)
             (case (first node)
               ((:const :bool :subquery-output) nil)
               (:subplan
                (let ((subplan (second node)))
                  (list (sql-subplan-kind subplan)
                        (statement-select-shape (sql-subplan-prepared subplan))
                        (shape (sql-subplan-test subplan)))))
               (:col
                (let* ((options (cddr node))
                       (key (getf options :aggregate-key))
                       (window (getf options :window-key))
                       (input (getf options :window-input-expression)))
                  (cond (key
                         (destructuring-bind (tag kind type operand) (sql-expression-key-value key)
                           (unless (eq tag :aggregate)
                             (throw 'unsupported-statement-fingerprint nil))
                           (list :aggregate kind type (shape operand))))
                        (window
                         (let ((descriptor (sql-expression-key-value window)))
                           (list :window (getf descriptor :kind)
                                 (shape (getf descriptor :input))
                                 (mapcar #'shape (getf descriptor :partition))
                                 (statement-order-shape (getf descriptor :order))
                                 (getf descriptor :frame))))
                        (input (list :window-input (shape input)))
                        ((getf options :set-function-expression)
                         (list (shape (getf options :set-function-expression))))
                        (t (list (second node))))))
               ((:parameter :sort-output) (list (second node)))
               (:is-null (list (third node) (shape (second node))))
               (:is-bool (list (third node) (fourth node) (shape (second node))))
               (:unary (list (second node) (shape (third node))))
               ((:cast :assignment :not) (list (shape (second node))))
               (:outer-ref (list (second node) (shape (third node))))
               ((:and :or :concat) (list (shape (second node)) (shape (third node))))
               ((:binop :any :all) (list (second node) (shape (third node)) (shape (fourth node))))
               (:in
                (let ((options (cddddr node)))
                  (list (fourth node)
                        (shape (getf options :array-left))
                        (array-items (getf options :array-items))
                        (mapcar (lambda (pair) (mapcar #'shape pair))
                                (getf options :comparisons)))))
               (:coalesce (mapcar #'shape (second node)))
               (:case
                   (list (mapcar (lambda (branch) (mapcar #'shape branch)) (second node))
                         (shape (third node))))
               (:array
                (list (getf (cddr node) :multidims) (array-items (second node))))
               (:array-subscript
                (list (shape (second node)) (mapcar #'shape (third node))))
               (:array-slice
                (list (shape (second node))
                      (mapcar (lambda (bounds) (mapcar #'shape bounds)) (third node))))
               ((:func :set-func :single-record) (list (second node) (mapcar #'shape (third node))))
               (:array-length (list (shape (second node)) (shape (third node))))
               (:record-field (list (third node) (shape (second node))))
               (otherwise (throw 'unsupported-statement-fingerprint nil)))))
    (shape expression)))

(defun statement-order-shape (keys)
  (mapcar (lambda (key)
            (cons (statement-expression-shape (car key)) (cdr key)))
          keys))

(defvar *statement-cte-frames* nil
  "Lexical CTE definition lists for the query shapes currently being fingerprinted.")

(defun statement-cte-reference-shape (definition worktable-p)
  (loop for frame in *statement-cte-frames*
        for depth from 0
        for slot = (position definition frame :test #'eq)
        when slot return (list :cte depth slot worktable-p)
          finally (throw 'unsupported-statement-fingerprint nil)))

(defun call-with-statement-cte-shape (prepared body-shape)
  "Wrap a statement body with its lexical WITH definitions, when present."
  (declare (dynamic-extent body-shape))
  (let ((definitions (getf prepared :ctes)))
    (if definitions
        (let ((*statement-cte-frames* (cons definitions *statement-cte-frames*)))
          (list :with
                (mapcar (lambda (definition)
                          (list (sql-cte-name definition)
                                (sql-cte-materialized definition)
                                (statement-select-shape (sql-cte-prepared definition))))
                        definitions)
                (funcall body-shape prepared)))
        (funcall body-shape prepared))))

(defun statement-select-shape (prepared)
  (call-with-statement-cte-shape prepared #'statement-select-body-shape))

(defun statement-select-body-shape (prepared)
  "Identify a resolved SELECT body independent of its physical access paths."
  (when (getf prepared :recursive-placeholder)
    (throw 'unsupported-statement-fingerprint nil))
  (when (getf prepared :set-operation)
    (return-from statement-select-body-shape
      (list :set-operation (getf prepared :set-operation)
            (statement-select-shape (getf prepared :left))
            (statement-select-shape (getf prepared :right))
            (mapcar (lambda (target) (list (getf target :type) (getf target :typmod)))
                    (getf prepared :targets))
            (statement-order-shape (getf prepared :order))
            (not (null (getf prepared :limit))))))
  (list* (if (getf prepared :values-query-p) :values :select)
         (mapcar (lambda (relation)
                   (let ((table (getf relation :table))
                         (subquery (getf relation :subquery-prepared)))
                     (cond ((getf relation :cte)
                            (statement-cte-reference-shape (getf relation :cte)
                                                           (getf relation :cte-worktable)))
                           (subquery (list :subquery (statement-select-shape subquery)))
                           (table (pg-class-oid table))
                           (t (throw 'unsupported-statement-fingerprint nil)))))
                 (getf prepared :source-relations))
         (mapcar (lambda (target)
                   (statement-expression-shape (getf target :expr)))
                 (getf prepared :targets))
         (statement-expression-shape
          (if (getf prepared :has-having)
              (car (getf prepared :qualification-identity))
              (getf prepared :where)))
         (getf prepared :distinct)
         (not (null (getf prepared :limit)))
         (append
          (when (getf prepared :joins)
            (list :joins
                  (mapcar (lambda (join)
                            (list (getf join :type)
                                  (statement-expression-shape (getf join :qual))))
                          (getf prepared :joins))))
          (when (getf prepared :order)
            (list (statement-order-shape (getf prepared :order))))
          (when (getf prepared :group-keys)
            (list :group-by (mapcar #'statement-expression-shape (getf prepared :group-keys))))
          (when (getf prepared :has-having)
            (list :having (statement-expression-shape
                           (cdr (getf prepared :qualification-identity))))))))

(defun statement-shape-id (shape &optional (version +statement-fingerprint-version+))
  "Hash typed shape fields without allocating intermediate arbitrary-precision words."
  (labels ((mix (hash word)
             (declare (type (unsigned-byte 64) hash word))
             (ldb (byte 64 0) (* (logxor hash word) #x100000001b3)))
           (mix-text (hash text)
             (declare (type (unsigned-byte 64) hash) (type string text))
             (let ((result (mix hash (length text))))
               (declare (type (unsigned-byte 64) result))
               (loop for char across text do (setf result (mix result (char-code char))))
               result))
           (walk (hash value)
             (declare (type (unsigned-byte 64) hash))
             (etypecase value
               (null (mix hash 0))
               (cons (walk (walk (mix hash 1) (car value)) (cdr value)))
               (integer (mix (mix hash 2) (ldb (byte 64 0) value)))
               (string (mix-text (mix hash 3) value))
               (symbol (mix-text (mix hash 4) (symbol-name value))))))
    (declare (inline mix mix-text)
             (ftype (function ((unsigned-byte 64) t) (unsigned-byte 64)) walk))
    (let ((id (ldb (byte 63 0)
                   (walk (mix #xcbf29ce484222325 version) shape))))
      (if (zerop id) 1 id))))

(defun statement-select-fingerprint (prepared)
  "Return an opaque workload identifier. Unsupported shapes return NIL."
  (catch 'unsupported-statement-fingerprint
    (statement-shape-id (statement-select-shape prepared))))

(defun statement-assignment-shape (expression)
  (if (eq expression :default)
      :default
      (statement-expression-shape expression)))

(defun statement-conflict-shape (conflict)
  (list :on-conflict
        (getf conflict :action)
        (getf conflict :inference-columns)
        (when (getf conflict :constraint) (first (getf conflict :arbiters)))
        (statement-expression-shape (getf conflict :arbiter-predicate))
        (mapcar (lambda (assignment)
                  (list (pg-column-attnum (car assignment))
                        (statement-assignment-shape (getf (cdr assignment) :expr))))
                (getf conflict :sets))
        (statement-expression-shape (getf (getf conflict :where-target) :expr))))

(defun statement-dml-body-shape (kind prepared)
  (unless (member kind '(:insert :update :delete))
    (throw 'unsupported-statement-fingerprint nil))
  (list* kind
         (pg-class-oid (getf prepared :table))
         (getf prepared :attnums)
         (mapcar (lambda (row) (mapcar #'statement-assignment-shape row)) (getf prepared :rows))
         (mapcar (lambda (item)
                   (list (pg-column-attnum (car item)) (statement-assignment-shape (cdr item))))
                 (getf prepared :sets))
         (statement-expression-shape (getf prepared :where))
         (mapcar (lambda (target) (statement-expression-shape (getf target :expr)))
                 (getf prepared :returning))
         (when (getf prepared :insert-source)
           (list (statement-select-shape (getf prepared :insert-source))
                 (mapcar (lambda (target) (statement-assignment-shape (getf target :expr)))
                         (getf prepared :source-assignments))))
         (append (when (getf prepared :dml-source)
                   (list (statement-select-shape (getf prepared :dml-source))))
                 (when (getf prepared :conflict)
                   (list (statement-conflict-shape (getf prepared :conflict)))))))

(defun statement-dml-fingerprint (kind prepared)
  "Identify analyzed DML by target, assignments, qualification and RETURNING."
  (catch 'unsupported-statement-fingerprint
    (statement-shape-id
     (call-with-statement-cte-shape
      prepared (lambda (query) (statement-dml-body-shape kind query))))))
