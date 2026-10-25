(in-package #:pg-interop-tests)

(defun copy-contract-sqlstate (sql)
  (handler-case (progn (scalar-sql-value sql) nil)
    (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))

(deftest initial-copy-scalar-expressions
    (dolist (case '(("SELECT CASE WHEN true THEN 7 ELSE 1/0 END" 7)
                    ("SELECT CASE WHEN NULL THEN 1 WHEN true THEN 2 ELSE 3 END" 2)
                    ("SELECT CASE WHEN false THEN 1 END" :sql-null)
                    ("SELECT CASE WHEN true THEN NULL ELSE 3 END" :sql-null)
                    ("SELECT 32767::int2 + 0::int2" 32767)
                    ("SELECT (-32768)::int2" -32768)
                    ("SELECT 2::int2 IN (1,2,3)" t)
                    ("SELECT 'b' = ANY(ARRAY['a','b'])" t)
                    ("SELECT 'b' = ANY(ARRAY['a',NULL])" :sql-null)
                    ("SELECT 'b' = ANY(ARRAY[]::text[])" nil)
                    ("SELECT NULL = ANY(ARRAY[]::text[])" nil)
                    ("SELECT 'b' = ANY(NULL::text[])" :sql-null)
                    ("SELECT array_length(ARRAY['a',NULL],1)" 2)
                    ("SELECT array_length(ARRAY[]::text[],1)" :sql-null)
                    ("SELECT array_length(ARRAY['a'],2)" :sql-null)))
      (ok (equalp (second case) (scalar-sql-value (first case))) (first case)))
  (dolist (case '(("SELECT 32768::int2" "22003")
                  ("SELECT 32767::int2 + 1::int2" "22003")
                  ("SELECT CASE WHEN 1 THEN 2 END" "42804")
                  ("SELECT ARRAY[]" "42P18")
                  ("SELECT 2::int2 = ANY(ARRAY['2'])" "42883")))
    (ok (equal (second case) (copy-contract-sqlstate (first case))) (first case))))

(deftest native-tablesync-catalog-expressions
    (call-with-publication-srf-fixture
     (lambda (database)
       (declare (ignore database))
       (ok (equalp '(#(201 :sql-null))
                   (derived-query-rows
                    "SELECT gpt.relid, CASE WHEN array_length(gpt.attrs,1)=c.relnatts THEN NULL ELSE gpt.attrs END FROM pg_get_publication_tables(ARRAY['p'],201) gpt, pg_class c WHERE c.oid=gpt.relid")))
       (ok (equalp '(#(1 "id" 23 :sql-null nil) #(2 "label" 1043 :sql-null nil))
                   (derived-query-rows
                    "SELECT a.attnum,a.attname,a.atttypid,a.attnum=ANY(i.indkey),a.attgenerated!='' FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_index i ON i.indexrelid=pg_get_replica_identity_index(201) WHERE a.attnum>0::pg_catalog.int2 AND NOT a.attisdropped AND a.attrelid=201 ORDER BY a.attnum")))
       (ok (null (derived-query-rows
                  "SELECT gpt.relid FROM pg_get_publication_tables(ARRAY['missing'],201) gpt")))
       (ok (equalp '(#(:sql-null))
                   (derived-query-rows
                    "SELECT DISTINCT pg_get_expr(gpt.qual,gpt.relid) FROM pg_get_publication_tables(ARRAY['p'],201) gpt"))))))

(deftest copy-text-output-escaping
    ;; Compare the escaping table in C copyto.c CopyAttributeOutText.
    (flet ((encode (text)
             (with-output-to-string (output) (pg::copy-text-field text output))))
      (ok (string= "\\N" (encode nil)))
      (ok (string= "\\\\N" (encode "\\N")))
      (ok (string= "\\b\\t\\n\\v\\f\\r\\\\"
                   (encode (coerce (mapcar #'code-char '(8 9 10 11 12 13 92)) 'string))))
      (ok (string= "żółw" (encode "żółw")))
      (ok (handler-case (progn (encode (string (code-char 0))) nil)
            (pg::pg-error (condition)
              (string= "22021" (pg::pg-error-sqlstate condition)))))))
