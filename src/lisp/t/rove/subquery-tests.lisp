;;;; subquery-tests.lisp -- query boundaries and expression subplans.
(in-package #:pg)

(rove:deftest subquery-consumer-stop-crosses-limit
    (let ((visited 0))
      (execute-physical-plan-node
       '(:limit 10 (:result)) nil nil
       (lambda (row) (declare (ignore row)) :stop)
       (lambda (node emit)
         (declare (ignore node))
         (loop for value below 20
               do (incf visited)
               when (eq (funcall emit value) :stop) do (return :stop)))
       (lambda (&rest arguments) (declare (ignore arguments))))
      (rove:ok (= visited 1) "the consumer stops LIMIT before its own bound")))

(rove:deftest subquery-consumer-stop-crosses-append
    (let ((opened 0) (visited 0))
      (stream-union-all
       '((() first) (() second))
       (lambda (row) (declare (ignore row)) :stop)
       (lambda (node emit)
         (declare (ignore node))
         (incf opened)
         (incf visited)
         (funcall emit (cons #() #()))))
      (rove:ok (= opened 1) "consumer stop prevents opening the next arm")
      (rove:ok (= visited 1))))

(rove:deftest buffered-plan-clears-toast-demand-for-consumer
    (let ((original (symbol-function 'simple-select-toast-demand))
          (seen :unreached)
          (targets (list (list :expr '(:const 1 :type :int4 :typmod -1))))
          (order (list (cons '(:sort-output 1) nil))))
      (unwind-protect
           (progn
             ;; A synthetic demand isolates the outward-callback contract from storage.
             (setf (symbol-function 'simple-select-toast-demand)
                   (lambda (node relations) (declare (ignore node relations)) (list :sentinel)))
             (dolist (kind '(:sort :top-n))
               (setf seen :unreached)
               (let* ((project (list :project targets '(:result)))
                      (node (if (eq kind :sort) (list :sort order project)
                                (list :top-n order 1 project)))
                      (plan (%make-query-plan :physical node)))
                 (interpret-query-plan plan nil
                                       (lambda (row)
                                         (declare (ignore row))
                                         (setf seen *select-toast-demand*)))
                 (rove:ok (null seen) (format nil "~a consumer sees no scan demand" kind)))))
        (setf (symbol-function 'simple-select-toast-demand) original))))

(rove:deftest bytea-substring-overloads
    (dolist (case '(("SELECT substr('\\x01020304'::bytea,2,2)::text" "\\x0203")
                    ("SELECT substring('\\x01020304'::bytea,3)::text" "\\x0304")
                    ("SELECT substr('\\x01020304'::bytea,0,2)::text" "\\x01")
                    ("SELECT substr('\\x01020304'::bytea,100,2)::text" "\\x")
                    ("SELECT substr('\\x01020304'::bytea,2,2147483647)::text" "\\x020304")
                    ("SELECT substr('\\x01020304'::bytea,2147483647,2147483647)::text" "\\x")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case)))
               (first case)))
  (rove:ok (equal (list (list +null+)) (subquery-test-rows "SELECT substr(NULL::bytea,1,1)")))
  (dolist (case '(("SELECT substr('\\x01'::bytea,1,-1)" "22011")
                  ("SELECT substr(DISTINCT '\\x01'::bytea,'bad')" "42809")
                  ("SELECT substr('\\x01'::bytea,'bad' ORDER BY 1)" "42809")))
    (let ((error (handler-case (progn (subquery-test-rows (first case)) nil)
                   (pg-error (error) error))))
      (rove:ok (and error (string= (second case) (pg-error-sqlstate error))) (first case)))))

(rove:deftest bytea-concatenation
    (dolist (case '(("SELECT ('\\x0001'::bytea || '\\xff'::bytea)::text" "\\x0001ff")
                    ("SELECT ('\\x01'::bytea || '\\x0203')::text" "\\x010203")
                    ("SELECT ('\\x01' || '\\x0203'::bytea)::text" "\\x010203")
                    ("SELECT ('\\x'::bytea || '\\x00'::bytea)::text" "\\x00")
                    ("SELECT 'x' || 'y'" "xy")
                    ("SELECT ('x'::bytea || 'y'::text)" "\\x78y")
                    ("SELECT ('y'::text || 'x'::bytea)" "y\\x78")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (dolist (sql '("SELECT NULL || '\\x01'::bytea" "SELECT '\\x01'::bytea || NULL"))
    (rove:ok (equal (list (list +null+)) (subquery-test-rows sql)) sql))
  (rove:ok (= (sql-bytea-concat-size 1 1073741818) 1073741819))
  (let ((condition (handler-case (progn (sql-bytea-concat-size 1 1073741819) nil)
                     (pg-error (error) error))))
    (rove:ok (and condition (string= (pg-error-sqlstate condition) "XX000")
                  (string= (pg-error-message condition) "invalid memory alloc request size 1073741824"))))
  (dolist (case '(("SELECT '\\x01'::bytea || ARRAY[1]" "42883")
                  ("SELECT ARRAY[1] || '\\x01'::bytea" "42883")
                  ("SELECT '\\x01'::bytea || 1" "42883")
                  ("SELECT '\\x01'::bytea || '\\x0'" "22023")))
    (let ((condition (handler-case (progn (subquery-test-rows (first case)) nil)
                       (pg-error (error) error))))
      (rove:ok (and condition (string= (pg-error-sqlstate condition) (second case))) (first case)))))

(rove:deftest subquery-parser-forms
    (dolist (sql '("SELECT (SELECT 1)" "SELECT ((SELECT 1))"
                   "SELECT ((SELECT 1) UNION ALL SELECT 2)"
                   "SELECT EXISTS (SELECT 1)" "SELECT NOT EXISTS (SELECT 1)"
                   "SELECT 1 IN (SELECT 1)" "SELECT 1 NOT IN ((SELECT 1))"
                   "SELECT 1 = ANY (SELECT 1)" "SELECT 1 = SOME (SELECT 1)"
                   "SELECT 1 <> ALL (SELECT 1)" "SELECT ARRAY(SELECT 'x')"))
      (rove:ok (eq :select (getf (first (parse-statements sql)) :type)) sql)))

(rove:deftest subquery-parser-expression-continuations
    (dolist (sql '("SELECT 1 IN ((SELECT 1), 2)"
                   "SELECT 2 IN ((SELECT 1)+1)"
                   "SELECT 1 = ANY ((SELECT '{1}'::text[]))"))
      (rove:ok (first (parse-statements sql)) sql))
  (rove:ok (getf (prepare-select (first (parse-statements
                                         "SELECT sum(d.x) = ANY(SELECT 1) FROM (SELECT 1 AS x) d")))
                 :aggregate-p)))

(rove:deftest subquery-analysis-boundaries
    (dolist (sql '("SELECT (SELECT d.x) FROM (SELECT 1 AS x) d"
                   "SELECT (SELECT (SELECT d.x)) FROM (SELECT 1 AS x) d"
                   "SELECT (SELECT count(*) FROM (SELECT 1 AS x) d)"
                   "SELECT sum((SELECT sum(i.x) FROM (SELECT 1 AS x) i))"
                   "SELECT EXISTS(SELECT 1, 2)" "SELECT ARRAY(SELECT 'x')"))
      (let* ((prepared (prepare-select (first (parse-statements sql))))
             (target (first (getf prepared :targets))))
        (rove:ok target sql)))
  (let* ((prepared (prepare-select (first (parse-statements
                                           "SELECT (SELECT 'x' AS child_name)"))))
         (target (first (getf prepared :targets))))
    (rove:ok (string= (getf target :name) "child_name"))
    (rove:ok (eq (getf target :type) :text))
    (rove:ok (zerop (getf target :table-oid)))))

(rove:deftest subquery-analysis-frames-do-not-escape
    (let* ((prepared (prepare-select (first (parse-statements
                                             "SELECT sum((SELECT 1))"))))
           (descriptor (second (getf (first (getf prepared :aggregates)) :input)))
           (child (sql-subplan-prepared descriptor)))
      (rove:ok (null (select-scope-analysis-frame (getf prepared :scope))))
      (rove:ok (null (select-scope-analysis-frame (getf child :scope))))
      (rove:ok (null (select-scope-analysis-frame
                      (select-scope-parent (getf child :scope)))))))

(rove:deftest subquery-dml-analysis-frames-do-not-escape
    (let* ((*catalog* (make-catalog))
           (*backend* (make-backend :database "postgres"))
           (database (make-database :name "postgres" :oid 1))
           (table (make-pg-class
                   :oid 9000 :name "subquery_frame_test" :relfilenode 9000
                   :columns (vector (make-pg-column :name "id" :type :int8
                                                    :typmod -1 :attnum 1)))))
      (setf (gethash "postgres" (catalog-databases *catalog*)) database
            (gethash "subquery_frame_test" (database-tables database)) table)
      (let* ((prepared (prepare-dml-subqueries
                        (first (parse-statements
                                "INSERT INTO subquery_frame_test VALUES ((SELECT sum((SELECT 1))))"))))
             (descriptor (first (getf prepared :sublinks)))
             (child (sql-subplan-prepared descriptor))
             (nested (second (getf (first (getf child :aggregates)) :input))))
        (rove:ok (null (select-scope-analysis-frame (getf prepared :scope))))
        (rove:ok (null (select-scope-analysis-frame (getf child :scope))))
        (rove:ok (null (select-scope-analysis-frame
                        (getf (sql-subplan-prepared nested) :scope)))))))

(rove:deftest subquery-correlated-group-validation
    (let ((prepared (prepare-select (first (parse-statements
                                            "SELECT d.x, (SELECT d.x) FROM (SELECT 1 AS x) d GROUP BY d.x")))))
      (rove:ok (= (length (getf prepared :targets)) 2)))
  (rove:ok (handler-case
               (progn (prepare-select (first (parse-statements
                                              "SELECT count(*), (SELECT d.x) FROM (SELECT 1 AS x) d")))
                      nil)
             (pg-error (condition) (string= (pg-error-sqlstate condition) "42803")))))

(defun subquery-test-rows (sql)
  "Execute virtual-only query fixtures without storage or backend registration."
  (let* ((plan (plan-select-query (first (parse-statements sql))))
         (relations (getf (query-plan-prepared plan) :relations))
         (rows nil))
    (execute-query-plan plan relations
                        (lambda (row) (push (coerce (car row) 'list) rows)))
    (nreverse rows)))

(defun subquery-test-expected (value)
  (cond ((eq value :null) +null+)
        ((consp value) (cons (subquery-test-expected (car value))
                             (subquery-test-expected (cdr value))))
        ((and (vectorp value) (not (stringp value)))
         (map 'vector #'subquery-test-expected value))
        (t value)))

(defun checkpoint-test-facts (next oid)
  (let ((start +wal-segment-size+))
    (list :checkpoint start :checkpoint-timeline 2 :state-time 1700001000
          :snapshot (list :start start :redo start :timeline 2 :prev-timeline 1
                          :next-xid next :next-oid oid :time 1700000000
                          :oldest-active-xid (+ (ash (ldb (byte 32 32) next) 32) 3)))))

(cl-spec:defproperty checkpoint-record-preserves-persisted-counters
    ((epoch (range integer 0 4294967295)) (low (range integer 3 4294967295))
                                          (oid-high (range integer 0 4294967295)) (oid-low (range integer 0 4294967295)))
  (:about native-control-checkpoint-record)
  (:trials (:normal 100))
  (let* ((next (+ (ash epoch 32) low))
         (oid (+ (ash oid-high 32) oid-low))
         (*next-xid* 3)
         (*wal-insert-timeline* 17)
         (record (funcall 'native-control-checkpoint-record (checkpoint-test-facts next oid)))
         (values (sql-record-values record)))
    (and (= 20 (length values))
         (= 2 (aref values 3))
         (= 1 (aref values 4))
         (string= (format nil "~d:~d" epoch low) (aref values 7))
         (= oid (aref values 8))
         (string= (format nil "~d" oid) (value->text (aref values 8) :oid8))
         (= 3 (aref values 13))
         (= (* (- 1700000000 946684800) 1000000) (aref values 19))
         (every (lambda (index) (eq +null+ (aref values index)))
                '(5 6 9 10 11 12 14 15 16 17 18)))))

(rove:deftest checkpoint-record-analysis-is-nullary-typed-and-lazy
    (let ((original (symbol-function 'native-control-read))
          (reads 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'native-control-read)
                   (lambda (directory) (declare (ignore directory)) (incf reads)
                           (checkpoint-test-facts (+ (ash 1 32) 9) 4294967301)))
             (let* ((plan (plan-select-query
                           (first (parse-statements "SELECT * FROM pg_catalog.pg_control_checkpoint()"))))
                    (targets (getf (query-plan-prepared plan) :targets)))
               (rove:ok (= 20 (length targets)))
               (rove:ok (= 6437 (type-oid (expr-type (getf (nth 8 targets) :expr)))))
               (rove:ok (zerop reads) "planning does not inspect control files"))
             (rove:ok (equal '((2 1 "1:9"))
                             (subquery-test-rows
                              "SELECT timeline_id,prev_timeline_id,next_xid FROM pg_control_checkpoint()")))
             (rove:ok (= 1 reads) "one scalar FROM row reads one control generation")
             (rove:ok (equal '(("\"4294967301\"" "\"3\""))
                             (subquery-test-rows
                              "SELECT to_json(next_oid),to_json(oldest_active_xid) FROM pg_control_checkpoint()"))))
        (setf (symbol-function 'native-control-read) original)))
  (dolist (sql '("SELECT pg_control_checkpoint(1)" "SELECT public.pg_control_checkpoint()"))
    (rove:ok (handler-case (progn (plan-select-query (first (parse-statements sql))) nil)
               (pg-error (condition) (string= "42883" (pg-error-sqlstate condition))))
             sql)))

(rove:deftest checkpoint-record-counter-property-and-json
    (unit-check-property 'checkpoint-record-preserves-persisted-counters)
  (let* ((record (funcall 'native-control-checkpoint-record
                          (checkpoint-test-facts (+ (ash 1 32) 9) 18446744073709551615)))
         (json (sql-to-json record :record)))
    (rove:ok (search "\"next_oid\":\"18446744073709551615\"" json))
    (rove:ok (search "\"oldest_active_xid\":\"3\"" json)))
  (let ((record (funcall 'native-control-checkpoint-record
                         '(:checkpoint 0 :checkpoint-timeline 1))))
    (rove:ok (= 1 (aref (sql-record-values record) 3)))
    (rove:ok (eq +null+ (aref (sql-record-values record) 7))))
  (let* ((record (native-control-checkpoint-record
                  (checkpoint-test-facts +max-xid-one-past+ 0)))
         (values (sql-record-values record)))
    (rove:ok (string= "exhausted" (aref values 7)))
    (rove:ok (string= "000000020000000000000001" (aref values 2))
             "a segment-boundary REDO names its containing segment"))
  (let ((facts (checkpoint-test-facts 3 0)))
    (setf (getf (getf facts :snapshot) :timeline) #xffffffff
          (getf (getf facts :snapshot) :prev-timeline) #x80000000)
    (let ((values (sql-record-values (native-control-checkpoint-record facts))))
      (rove:ok (= -1 (aref values 3)))
      (rove:ok (= -2147483648 (aref values 4)))
      (rove:ok (string= "FFFFFFFF0000000000000001" (aref values 2))))))

(cl-spec:defproperty oid8-input-preserves-unsigned-width
    ((number (range integer 0 18446744073709551615)))
  (:about sql-oid8-input)
  (:trials (:normal 100))
  (and (= number (funcall 'sql-oid8-input (format nil "~d" number)))
       (= (mod (- number) (ash 1 64))
          (funcall 'sql-oid8-input (format nil "-~d" number)))))

(rove:deftest oid8-text-input-preserves-width-and-rejects-invalid-values
    (unit-check-property 'oid8-input-preserves-unsigned-width)
  (rove:ok (equal '((18446744073709551615 -1 4294967295 t))
                  (subquery-test-rows
                   "SELECT (-1::bigint)::oid8,('18446744073709551615'::oid8)::bigint,('4294967295'::oid)::oid8,'18446744073709551615'::oid8 > '9223372036854775807'::oid8")))
  (dolist (case '(("4294967296" 4294967296) ("-1" 18446744073709551615)
                  ("0xFFFFFFFFFFFFFFFF" 18446744073709551615) ("017" 15) ("0b101" 5)))
    (rove:ok (= (second case) (funcall 'sql-oid8-input (first case)))))
  (dolist (case '(("" "22P02") ("08" "22P02") ("1_0" "22P02") ("0x" "22P02")
                  ("18446744073709551616" "22003") ("-18446744073709551616" "22003")))
    (rove:ok (handler-case (progn (funcall 'sql-oid8-input (first case)) nil)
               (pg-error (condition) (string= (second case) (pg-error-sqlstate condition)))))))

(cl-spec:defproperty do-options-preserve-uncompiled-source
    ((value (range integer -10000 10000)) (language-first (member t nil)))
  (:about parse-statements)
  (:trials (:normal 100))
  (let* ((body (format nil "BEGIN PERFORM ~d; END;" value))
         (sql (if language-first
                  (format nil "DO LANGUAGE plpgsql $body$~a$body$" body)
                  (format nil "DO $body$~a$body$ LANGUAGE 'plpgsql'" body)))
         (ast (first (parse-statements sql)))
         (options (getf ast :options)))
    (and (eq (getf ast :type) :do)
         (string= body (cdr (assoc :source options)))
         (string= "plpgsql" (cdr (assoc :language options))))))

(rove:deftest do-options-retain-source-until-execution
    (unit-check-property 'do-options-preserve-uncompiled-source)
  (dolist (body '("BEGIN PERFORM 1; END;" "not valid plpgsql" ""))
    (let ((ast (first (parse-statements (format nil "DO $body$~a$body$" body)))))
      (rove:ok (eq :do (getf ast :type)))
      (rove:ok (equal (list (cons :source body)) (getf ast :options)))))
  (let ((ast (first (parse-statements "DO LANGUAGE plpgsql 'a' LANGUAGE sql 'b'"))))
    (rove:ok (equal '((:language . "plpgsql") (:source . "a")
                      (:language . "sql") (:source . "b"))
                    (getf ast :options))
             "C defers duplicate option validation to execution"))
  (dolist (sql '("DO" "DO LANGUAGE" "DO 1"))
    (rove:ok (handler-case (progn (parse-statements sql) nil)
               (pg-error (condition) (string= "42601" (pg-error-sqlstate condition))))
             sql)))

(rove:deftest do-block-compiles-supported-statements-and-nesting
    (let* ((block (compile-do-source
                   "BEGIN SET LOCAL synchronous_commit = 'off'; BEGIN NULL; PERFORM * FROM pg_catalog.txid_current(); END; RESET application_name; END;"))
           (statements (getf block :statements))
           (nested (getf (second statements) :statements)))
      (rove:ok (equal '(:set :do-block :reset)
                      (mapcar (lambda (statement) (getf statement :type)) statements)))
      (rove:ok (getf (first statements) :local-p))
      (rove:ok (equal '(:do-null :perform)
                      (mapcar (lambda (statement) (getf statement :type)) nested)))
      (rove:ok (eq :select (getf (getf (second nested) :query) :type))))
  (rove:ok (null (getf (compile-do-source "BEGIN END") :statements)))
  (dolist (source '("BEGIN NULL END;" "BEGIN" "BEGIN END; NULL;" "BEGIN BEGIN END END"))
    (rove:ok (handler-case (progn (compile-do-source source) nil)
               (pg-error (condition) (string= "42601" (pg-error-sqlstate condition))))
             source))
  (dolist (source '("BEGIN COMMIT; END;" "BEGIN EXECUTE 'SELECT 1'; END;"
                    "BEGIN IF true THEN NULL; END IF; END;"))
    (rove:ok (handler-case (progn (compile-do-source source) nil)
               (pg-error (condition) (string= "0A000" (pg-error-sqlstate condition))))
             source)))

(cl-spec:defproperty txid-current-preserves-full-xid-bits
    ((high (range integer 0 4294967295)) (low (range integer 3 4294967295)))
  (:about eval-func)
  (:trials (:normal 100))
  (let* ((xid (+ (ash high 32) low))
         (*backend* (make-backend :xid xid))
         (*physical-recovery-state* nil)
         (expected (if (>= xid (expt 2 63)) (- xid (expt 2 64)) xid)))
    (= expected (eval-func "txid_current" nil))))

(rove:deftest txid-current-analysis-does-not-allocate
    (let ((*backend* (make-backend)))
      (dolist (sql '("SELECT txid_current()" "SELECT * FROM pg_catalog.txid_current()"))
        (let* ((plan (plan-select-query (first (parse-statements sql))))
               (target (first (getf (query-plan-prepared plan) :targets))))
          (rove:ok (eq :int8 (expr-type (getf target :expr))) sql)
          (rove:ok (null (backend-xid *backend*)) "planning does not assign an XID")))
      (dolist (sql '("SELECT txid_current(1)" "SELECT public.txid_current()"))
        (rove:ok (handler-case
                     (progn (plan-select-query (first (parse-statements sql))) nil)
                   (pg-error (condition) (string= "42883" (pg-error-sqlstate condition))))
                 sql))))

(rove:deftest txid-current-reuses-transaction-owner-and-full-width
    (unit-check-property 'txid-current-preserves-full-xid-bits)
  (dolist (xid (list 3 (+ (ash 1 32) 3) (1- (ash 1 63)) (+ (ash 1 63) 3)))
    (let* ((*backend* (make-backend :xid xid))
           (*physical-recovery-state* nil)
           (expected (if (logbitp 63 xid) (- xid (ash 1 64)) xid)))
      (rove:ok (equal (list (list expected expected))
                      (subquery-test-rows "SELECT txid_current(),txid_current()")))
      (rove:ok (equal (list (list expected expected))
                      (subquery-test-rows "SELECT f,f.f FROM txid_current() f")))
      (rove:ok (= xid (backend-xid *backend*))))))

(rove:deftest txid-current-allocates-on-first-execution-and-rejects-recovery
    (let ((original (symbol-function 'assign-xid))
          (allocations 0)
          (*physical-recovery-state* nil)
          (*backend* (make-backend)))
      (unwind-protect
           (progn
             (setf (symbol-function 'assign-xid)
                   (lambda (&optional owner)
                     (incf allocations)
                     (setf (backend-xid owner) (+ (ash 1 32) 3))))
             (let* ((plan (plan-select-query
                           (first (parse-statements "SELECT txid_current(),txid_current()"))))
                    (relations (getf (query-plan-prepared plan) :relations))
                    (rows nil))
               (rove:ok (zerop allocations))
               (execute-query-plan plan relations (lambda (row) (push (car row) rows)))
               (rove:ok (= allocations 1) "one allocation for repeated calls")
               (rove:ok (equalp rows (list (vector (+ (ash 1 32) 3) (+ (ash 1 32) 3))))))
             (setf (backend-xid *backend*) nil)
             (let ((*physical-recovery-state* :standby))
               (rove:ok
                (handler-case (progn (eval-func "txid_current" nil) nil)
                  (pg-error (condition)
                    (and (string= "55000" (pg-error-sqlstate condition))
                         (search "pg_current_xact_id()" (princ-to-string condition)))))))
             (rove:ok (= allocations 1) "recovery is rejected before allocation")
             (rove:ok (null (backend-xid *backend*))))
        (setf (symbol-function 'assign-xid) original))))

(cl-spec:defproperty scalar-function-from-preserves-value-and-column-alias
    ((value (range integer -10000 10000)) (alias-index (range integer 0 2)))
  (:about parse-from-relation expand-select-targets)
  (:trials (:normal 100))
  (let* ((suffix (nth alias-index '("" " AS f" " AS f(v)")))
         (name (nth alias-index '("abs" "f" "v")))
         (sql (format nil "SELECT * FROM abs(~d)~a" value suffix))
         (plan (plan-select-query (first (parse-statements sql)))))
    (and (equal (list name) (mapcar (lambda (target) (getf target :name))
                                    (getf (query-plan-prepared plan) :targets)))
         (equal (list (list (abs value))) (subquery-test-rows sql)))))

(rove:deftest scalar-functions-in-from-preserve-types-values-and-aliases
    (unit-check-property 'scalar-function-from-preserves-value-and-column-alias)
  (dolist (entry '(("SELECT * FROM length('abcd')" ((4)))
                   ("SELECT f FROM length('abcd') AS f" ((4)))
                   ("SELECT f.v FROM length('abcd') AS f(v)" ((4)))
                   ("SELECT * FROM length(NULL::text)" ((:null)))
                   ("SELECT * FROM pg_is_in_recovery()" ((nil)))
                   ("SELECT * FROM abs((SELECT row_number() OVER ())) f" ((1)))
                   ("SELECT d.x,f.v FROM (SELECT 'a' AS x UNION ALL SELECT 'abcd') d, length(d.x) f(v) ORDER BY f.v" (("a" 1) ("abcd" 4)))
                   ("SELECT * FROM abs((SELECT sum(d.x) FROM (SELECT -2 AS x UNION ALL SELECT -3) d)) AS f" ((5)))))
    (rove:ok (equalp (subquery-test-expected (second entry))
                     (subquery-test-rows (first entry))) (first entry))))

(rove:deftest scalar-function-from-keeps-query-level-aggregate-and-window-guards
    (dolist (entry '(("SELECT * FROM sum(1)" . "42803")
                     ("SELECT * FROM abs(sum(1))" . "42803")
                     ("SELECT * FROM abs(row_number() OVER ())" . "42P20")
                     ("SELECT * FROM no_such_scalar_function()" . "42883")
                     ("SELECT * FROM row_number() OVER ()" . "42601")
                     ("SELECT * FROM length('a') f(a,b)" . "42P10")
                     ("SELECT (length('a')).*" . "42809")
                     ("SELECT * FROM (SELECT 1 AS x) d RIGHT JOIN abs(d.x) f(v) ON true" . "42P10")
                     ("SELECT * FROM (SELECT 1 AS x) d FULL JOIN abs(d.x) f(v) ON true" . "42P10")))
      (rove:ok (equal (cdr entry)
                      (handler-case (progn (subquery-test-rows (car entry)) nil)
                        (pg-error (condition) (pg-error-sqlstate condition))))
               (car entry))))

(rove:deftest scalar-function-whole-row-reference-keeps-scalar-shape
    (dolist (entry '(("SELECT f.v,f FROM length('abc') AS f(v)" ((3 3)))
                     ("SELECT to_json(f)::text,to_json(f.*)::text FROM length('abc') AS f(v)" (("3" "3")))
                     ("SELECT to_json(d)::text FROM (SELECT * FROM length('abc') f(v)) d" (("{\"v\":3}")))
                     ("SELECT (SELECT f) FROM length('abc') f(v)" ((3)))
                     ("SELECT to_json(f)::text FROM (SELECT 0 AS n) d LEFT JOIN LATERAL length('abc') f(v) ON false" ((:null)))))
      (rove:ok (equalp (subquery-test-expected (second entry))
                       (subquery-test-rows (first entry))) (first entry))))

(rove:deftest scalar-function-from-retains-type-typmod-and-virtual-origin
    (dolist (entry '(("SELECT * FROM pg_catalog.int2abs(-2::smallint) f(v)" :int2 "v")
                     ("SELECT * FROM numeric_abs(1.20::numeric(8,2)) f(v)" :numeric "v")
                     ("SELECT * FROM length('abc') AS \"MiX\"" :int4 "MiX")))
      (let* ((plan (plan-select-query (first (parse-statements (first entry)))))
             (target (first (getf (query-plan-prepared plan) :targets))))
        (rove:ok (eq (second entry) (getf target :type)))
        (rove:ok (equal (third entry) (getf target :name)))
        (rove:ok (= -1 (getf target :typmod)))
        (rove:ok (zerop (getf target :table-oid)))
        (rove:ok (zerop (getf target :attnum))))))

(rove:deftest scalar-function-from-rejects-nested-set-results
    (dolist (sql '("SELECT * FROM to_json(pg_stat_get_activity(NULL))"
                   "SELECT * FROM json_build_array(pg_stat_get_wal_senders())"))
      (rove:ok
       (equal "0A000" (handler-case (progn (subquery-test-rows sql) nil)
                        (pg-error (condition) (pg-error-sqlstate condition)))) sql)))

(rove:deftest scalar-function-from-plan-reuses-parameter-bindings
    (let* ((*query-parameter-types* #(:int4))
           (*query-parameter-values* #(-3))
           (plan (plan-select-query
                  (first (parse-statements "SELECT f.v,f FROM abs($1) f(v)"))))
           (relations (getf (query-plan-prepared plan) :relations))
           (rows nil))
      (execute-query-plan plan relations (lambda (row) (push (coerce (car row) 'list) rows)))
      (setf *query-parameter-values* #(-8))
      (execute-query-plan plan relations (lambda (row) (push (coerce (car row) 'list) rows)))
      (rove:ok (equal '((3 3) (8 8)) (nreverse rows)))))

(rove:deftest subquery-execution-results
    (dolist (case '(("SELECT (SELECT 7)" ((7)))
                    ("SELECT (SELECT 7 WHERE false)" ((:null)))
                    ("SELECT EXISTS(SELECT 1 WHERE false), NOT EXISTS(SELECT 1)" ((nil nil)))
                    ("SELECT 1 IN (SELECT 1 UNION ALL SELECT 2)" ((t)))
                    ("SELECT 3 NOT IN (SELECT 1 UNION ALL SELECT NULL)" ((:null)))
                    ("SELECT NULL = ANY (SELECT 1 WHERE false), NULL = ALL (SELECT 1 WHERE false)" ((nil t)))
                    ("SELECT (SELECT d.x+1) FROM (SELECT 4 AS x UNION ALL SELECT 8) d" ((5) (9)))
                    ("SELECT (SELECT (SELECT d.x)) FROM (SELECT 4 AS x) d" ((4)))
                    ("SELECT (SELECT s.x FROM (SELECT d.x AS x) s) FROM (SELECT 4 AS x) d" ((4)))
                    ("SELECT (SELECT count(*) FROM (SELECT 1 AS x) q)" ((1)))
                    ("SELECT sum((SELECT sum(i.x) FROM (SELECT 1 AS x) i))" ((1)))
                    ("SELECT d.x, (SELECT d.x) FROM (SELECT 4 AS x) d GROUP BY d.x" ((4 4)))
                    ("SELECT ARRAY(SELECT 'x' UNION ALL SELECT NULL)::text" (("{x,NULL}")))))
      (let ((expected (subquery-test-expected (second case))))
        (rove:ok (equalp (subquery-test-rows (first case)) expected) (first case)))))

(rove:deftest subquery-scalar-cardinality
    (rove:ok
     (handler-case
         (progn (subquery-test-rows "SELECT (SELECT 1 UNION ALL SELECT 2)") nil)
       (pg-error (condition) (string= (pg-error-sqlstate condition) "21000")))))

(rove:deftest subquery-aggregate-owner
    (rove:ok (equal '((3)) (subquery-test-rows
                            "SELECT (SELECT sum(d.x)) FROM (SELECT 1 AS x UNION ALL SELECT 2) d")))
  (rove:ok (equal '((4)) (subquery-test-rows
                          "SELECT (SELECT sum(d.x)+1) FROM (SELECT 1 AS x UNION ALL SELECT 2) d")))
  (rove:ok (handler-case
               (progn (subquery-test-rows
                       "SELECT sum((SELECT sum(d.x))) FROM (SELECT 1 AS x) d") nil)
             (pg-error (condition) (string= (pg-error-sqlstate condition) "42803"))))
  (rove:ok (handler-case
               (progn (subquery-test-rows
                       "SELECT (SELECT sum(d.x) FROM (SELECT 1 UNION ALL SELECT 2) i) FROM (SELECT 1 AS x) d") nil)
             (pg-error (condition) (string= (pg-error-sqlstate condition) "21000")))))

(rove:deftest subquery-distinct-grouped-output-order
    (rove:ok (equal '((1)) (subquery-test-rows "SELECT DISTINCT count(*) ORDER BY count(*)")))
  (rove:ok (equal '((1 1))
                  (subquery-test-rows
                   "SELECT DISTINCT d.y,count(*) FROM (SELECT 0 AS x,1 AS y) d GROUP BY d.y ORDER BY d.y")))
  (rove:ok (equal '((1 1))
                  (subquery-test-rows
                   "SELECT DISTINCT d.y,(SELECT d.y) AS correlated FROM (SELECT 0 AS x,1 AS y) d GROUP BY d.y ORDER BY correlated"))))

(rove:deftest subquery-distinct-repeated-expression
    (rove:ok (equal '((1))
                    (subquery-test-rows
                     "SELECT DISTINCT (SELECT d.x) FROM (SELECT 1 AS x) d ORDER BY (SELECT d.x)")))
  (rove:ok (equal '((2))
                  (subquery-test-rows
                   "SELECT DISTINCT (SELECT d.x)+1 FROM (SELECT 1 AS x) d ORDER BY (SELECT d.x)+1")))
  (rove:ok (equal '((1))
                  (subquery-test-rows
                   "SELECT DISTINCT (SELECT d.x) FROM (SELECT 1 AS x) d ORDER BY (SELECT x)")))
  (let* ((plan (plan-select-query
                (first (parse-statements
                        "SELECT DISTINCT (SELECT sum(o.x+nextval('unused'::text))) FROM (SELECT 1 AS x) o ORDER BY (SELECT sum(o.x+nextval('unused'::text)))")))))
    (rove:ok (= 1 (getf (query-plan-prepared plan) :aggregate-count))
             "ORDER reuses the selected outer-owned volatile transition")))

(rove:deftest subquery-semantic-nesting-and-final-dependencies
    (rove:ok (equal '((13))
                    (subquery-test-rows
                     "SELECT (SELECT sum(i.x + sum(o.x)) FROM (SELECT 10 AS x) i) FROM (SELECT 1 AS x UNION ALL SELECT 2) o")))
  (let* ((plan (plan-select-query
                (first (parse-statements
                        "SELECT (SELECT nextval('unused'::text) + CASE WHEN EXISTS(SELECT o.x) THEN 0 ELSE 0 END) FROM (SELECT 1 AS x) o"))))
         (descriptor (first (getf (query-plan-prepared plan) :sublinks))))
    (rove:ok (null (sql-subplan-depths descriptor))
             "EXISTS target removal precedes dependency classification")))

(cl-spec:defspec-function subquery-quantified-step
    (:args (kind (member :any :all))
           (accumulated (member nil t :sql-null))
           (value (member nil t :sql-null)))
  (:returns (member nil t :sql-null))
  (:post
   (eq result
       (if (eq kind :any)
           (cond ((or (eq accumulated t) (eq value t)) t)
                 ((or (eq accumulated +null+) (eq value +null+)) +null+)
                 (t nil))
           (cond ((or (null accumulated) (null value)) nil)
                 ((or (eq accumulated +null+) (eq value +null+)) +null+)
                 (t t))))))

(cl-spec:defproperty subquery-quantified-fold-truth
    ((digits (range integer 0 1048575))
     (size (range integer 0 12))
     (all-p boolean))
  (:about subquery-quantified-step)
  (:trials (:normal 300))
  (let* ((values (loop for index below size
                       collect (aref (vector nil t +null+) (mod (floor digits (expt 3 index)) 3))))
         (kind (if all-p :all :any))
         (actual (reduce (lambda (accumulated value)
                           (subquery-quantified-step kind accumulated value))
                         values :initial-value all-p))
         (expected (if all-p
                       (cond ((member nil values) nil)
                             ((member +null+ values) +null+)
                             (t t))
                       (cond ((member t values) t)
                             ((member +null+ values) +null+)
                             (t nil)))))
    (eq actual expected)))

(cl-spec:defproperty subquery-generated-scalar-cardinality
    ((size (range integer 0 6)) (value (range integer -1000 1000)))
  (:about eval-subquery-expression)
  (:trials (:normal 150))
  (let* ((child (if (zerop size) "SELECT 1 WHERE false"
                    (format nil "~{~a~^ UNION ALL ~}"
                            (loop repeat size collect (format nil "SELECT ~d" value)))))
         (sql (format nil "SELECT (~a)" child)))
    (handler-case
        (let ((rows (subquery-test-rows sql)))
          (and (<= size 1)
               (equal rows (list (list (if (zerop size) +null+ value))))))
      (pg-error (condition)
        (and (> size 1) (string= (pg-error-sqlstate condition) "21000"))))))

(rove:deftest subquery-generated-scalar-cardinality-check
    (unit-check-property 'subquery-generated-scalar-cardinality))

(rove:deftest subquery-generated-quantified-truth
    (let ((data (cl-spec:result-data
                 (cl-spec:check-function 'subquery-quantified-step :trials 300 :seed 42))))
      (rove:ok (eq (getf data :status) :passed) (format nil "~s" data)))
  (unit-check-property 'subquery-quantified-fold-truth))

(rove:deftest subquery-value-and-cache-ownership
    (let* ((record (make-sql-record :fields '((:name "x" :type :text)) :values #("kept")))
           (copy (copy-subquery-value record)))
      (rove:ok (equalp (sql-record-fields copy) (sql-record-fields record)))
      (rove:ok (equalp (sql-record-values copy) #("kept")))
      (rove:ok (not (eq (sql-record-values copy) (sql-record-values record)))))
  (let* ((*subquery-result-cache* (make-hash-table :test #'eq))
         (owner (make-hash-table :test #'eq))
         (*subquery-outer-frames* (list (cons #(7) owner)))
         (descriptor (make-sql-subplan :kind :scalar :depths '(2))))
    (rove:ok (eq owner (subquery-cache-for descriptor)))
    (setf (sql-subplan-depths descriptor) nil)
    (rove:ok (eq *subquery-result-cache* (subquery-cache-for descriptor)))
    (setf (sql-subplan-depths descriptor) '(1))
    (rove:ok (null (subquery-cache-for descriptor)))))

(rove:deftest subquery-explain-and-instrumentation
    (let* ((plan (plan-select-query
                  (first (parse-statements
                          "SELECT (SELECT d.x) FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) d"))))
           (relations (getf (query-plan-prepared plan) :relations))
           (descriptor (first (getf (query-plan-prepared plan) :sublinks)))
           (child (sql-subplan-plan descriptor))
           (*query-plan-instrumentation-p* t))
      (execute-query-plan plan relations (lambda (row) (declare (ignore row))))
      (rove:ok (= 3 (plan-node-metadata-actual-loops
                     (gethash (query-plan-physical child) (query-plan-metadata child)))))
      (rove:ok (search "SubPlan" (format-query-plan plan relations t)))
      (execute-query-plan plan relations (lambda (row) (declare (ignore row))))
      (rove:ok (= 3 (plan-node-metadata-actual-loops
                     (gethash (query-plan-physical child) (query-plan-metadata child))))
               "a new execution resets, while rescans accumulate"))
  (let ((plan (plan-select-query (first (parse-statements "SELECT (SELECT 1)")))))
    (rove:ok (search "InitPlan" (format-query-plan plan)))))

(rove:deftest subquery-inherits-compiler-hook
    (let ((compilations 0))
      (unwind-protect
           (progn
             (register-query-plan-backend
              :subquery-test-compiler
              :compiler (lambda (plan)
                          (incf compilations)
                          (lambda (relations callback)
                            (interpret-query-plan plan relations callback))))
             (let* ((plan (plan-select-query (first (parse-statements "SELECT (SELECT 1)"))))
                    (relations (getf (query-plan-prepared plan) :relations)))
               (execute-query-plan plan relations (lambda (row) (declare (ignore row)))
                                   :backend :subquery-test-compiler)
               (rove:ok (= compilations 2) "root and child inherit the compiler entry")))
        (sb-thread:with-mutex (*query-plan-backend-lock*)
          (remhash :subquery-test-compiler *query-plan-backends*)))))

(rove:deftest subquery-mixed-registration-is-atomic
    (let ((*relation-writers* (make-hash-table :test #'eql))
          (*relation-readers* (make-hash-table :test #'eql))
          (*relation-builders* (make-hash-table :test #'eql))
          (*relation-reader-blockers* (make-hash-table :test #'eql))
          (*relation-build-groups* (make-hash-table :test #'eq))
          (*backend* (make-backend :statement-deadline (1- (get-internal-real-time)))))
      (setf (gethash 20 *relation-reader-blockers*) :blocked)
      (rove:ok (handler-case (progn (register-relation-accesses '(10) '(10 20) 42) nil)
                 (pg-error (condition) (string= (pg-error-sqlstate condition) "57014"))))
      (rove:ok (null (gethash 10 *relation-writers*)) "blocked reads install no partial writer")
      (remhash 20 *relation-reader-blockers*)
      (register-relation-accesses '(10) '(10 20) 42)
      (rove:ok (equal '(42) (gethash 10 *relation-writers*)))
      (rove:ok (null (gethash 10 *relation-readers*)) "self-read needs no read-to-write upgrade")
      (rove:ok (equal '(42) (gethash 20 *relation-readers*)))))

(rove:deftest subquery-version-key-detects-change
    (let ((tuple (make-htup :block 2 :offnum 3 :xmin 4 :xmax 0 :cid 1
                            :ctid-block 2 :ctid-offnum 3 :values #(8))))
      (let ((before (heap-tuple-version-key tuple)))
        (rove:ok (equal before (heap-tuple-version-key tuple)))
        (setf (htup-xmax tuple) 9)
        (rove:ok (equal before (heap-tuple-version-key tuple))
                 "an aborted writer's xmax does not change the target version")
        (setf (htup-offnum tuple) 4)
        (rove:ok (not (equal before (heap-tuple-version-key tuple)))))))
