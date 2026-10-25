;;;; COALESCE is a lazy SQL expression, not an eagerly evaluated function.
(in-package #:pg-physical-tests)

(defun coalesce-test-expression (sql)
  (pg::analyze-expr
   (getf (first (getf (first (pg::parse-statements sql)) :targets)) :expr) nil))

(deftest coalesce-resolves-types-and-coerces-all-inputs
    (let ((expression (coalesce-test-expression "SELECT COALESCE(NULL, '0/10'::pg_lsn, '0/20')")))
      (ok (eq :pg-lsn (pg::expr-type expression)))
      (ok (= 16 (pg::eval-expr expression nil))))
  (let ((expression (coalesce-test-expression "SELECT COALESCE(NULL, NULL)")))
    (ok (eq :text (pg::expr-type expression)))
    (ok (eq pg::+null+ (pg::eval-expr expression nil))))
  (let ((expression (coalesce-test-expression "SELECT COALESCE(NULL, 1, 2::bigint)")))
    (ok (eq :int8 (pg::expr-type expression)))
    (ok (= 1 (pg::eval-expr expression nil))))
  (let ((expression (coalesce-test-expression
                     "SELECT COALESCE(NULL, pg_wal_lsn_diff('0/10','0/0'), 0)")))
    (ok (eq :numeric (pg::expr-type expression)))
    (ok (= 16 (pg::eval-expr expression nil))))
  (ok (equal "22P02" (recovery-state-of
                      (lambda () (coalesce-test-expression "SELECT COALESCE('0/1'::pg_lsn, 'invalid')")))))
  (ok (equal "42804" (recovery-state-of
                      (lambda () (coalesce-test-expression "SELECT COALESCE(1, true)")))))
  (ok (equal "42601" (recovery-state-of
                      (lambda () (coalesce-test-expression "SELECT COALESCE()"))))))

(deftest coalesce-stops-after-first-non-null-including-false-and-zero
    (dolist (example '(("SELECT COALESCE(false, current_setting('missing.setting')::bool)" nil)
                       ("SELECT COALESCE(0, current_setting('missing.setting')::int)" 0)))
      (ok (eql (second example)
               (pg::eval-expr (coalesce-test-expression (first example)) nil))))
  (ok (equal "42704" (recovery-state-of
                      (lambda () (pg::eval-expr
                                  (coalesce-test-expression
                                   "SELECT COALESCE(NULL, current_setting('missing.setting'))") nil))))))

(deftest coalesce-preserves-common-typmod-and-column-name
    (ok (= 8 (pg::expr-typmod
              (coalesce-test-expression "SELECT COALESCE('a'::varchar(4), 'b'::varchar(4))"))))
  (ok (= -1 (pg::expr-typmod
             (coalesce-test-expression "SELECT COALESCE('a'::varchar(4), 'b'::varchar(5))"))))
  (dolist (sql '("SELECT COALESCE(NULL, 1)" "SELECT COALESCE(NULL, 1)::bigint"))
    (let ((raw (getf (first (getf (first (pg::parse-statements sql)) :targets)) :expr)))
      (ok (equal "coalesce" (pg::target-default-name raw))))))

(cl-spec:defproperty coalesce-selects-first-non-null-value
    ((a (range integer -100000 100000)) (b (range integer -100000 100000))
                                        (c (range integer -100000 100000)) (mask (range integer 0 7)))
  (:about pg::eval-coalesce-expression)
  (:trials (:normal 200))
  (let* ((values (loop for value in (list a b c) for index from 0
                       collect (if (logbitp index mask) pg::+null+ value)))
         (expression (list :coalesce
                           (mapcar (lambda (value) (list :const value :type :int4 :typmod -1)) values)
                           :type :int4 :typmod -1)))
    (eql (or (find-if (lambda (value) (not (eq value pg::+null+))) values) pg::+null+)
         (pg::eval-coalesce-expression expression nil))))

(deftest generated-coalesce-null-patterns
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'coalesce-selects-first-non-null-value :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
