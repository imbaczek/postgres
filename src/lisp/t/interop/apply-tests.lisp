(defpackage #:pg-interop-apply-tests (:use #:cl #:rove))
(in-package #:pg-interop-apply-tests)

(defmacro with-reference-clients ((client reference) &body body)
  "Close both peers on exit, including a failed second connection."
  `(let ((,client (pg::pg-client-connect
                   "127.0.0.1" (parse-integer
                                (or (uiop:getenv "PG_INTEROP_TARGET_PORT")
                                    (error "isolated target port required"))) "postgres")))
     (unwind-protect
          (let ((,reference (pg::pg-client-connect
                             "127.0.0.1" (parse-integer
                                          (or (uiop:getenv "PG_INTEROP_REFERENCE_PORT")
                                              (error "unmodified C reference port required")))
                             "postgres" :user
                             (or (uiop:getenv "PG_INTEROP_REFERENCE_USER") "postgres"))))
            (unwind-protect (progn ,@body) (pg::pg-client-close ,reference)))
       (pg::pg-client-close ,client))))

(defun observed-query-wire (client sql)
  "Retain successful wire frames verbatim; compare error SQLSTATE separately."
  (pg::pg-client-send-query client sql)
  (let ((frames nil) (state :ok))
    (loop
      (multiple-value-bind (kind payload) (pg::pg-client-frame (pg::pg-client-stream client))
        (case kind
          (#\E (setf state (pg::pg-client-error-sqlstate
                            (pg::pg-client-error-from-payload payload))))
          ((#\N #\S) nil)
          (otherwise (push (cons kind payload) frames)))
        (when (char= kind #\Z)
          (return (if (eq state :ok) (cons state (nreverse frames)) state)))))))

(defun native-publication-discovery-sql (publication-list)
  (format nil "SELECT DISTINCT n.nspname,c.relname,c.relkind,gpt.attrs
FROM pg_class c
JOIN pg_namespace n ON n.oid=c.relnamespace
JOIN (SELECT (pg_get_publication_tables(VARIADIC array_agg(pubname::text))).*
      FROM pg_publication WHERE pubname IN (~a)) AS gpt ON gpt.relid=c.oid
UNION ALL
SELECT DISTINCT s.schemaname,s.sequencename,'S'::\"char\" AS relkind,NULL::int2vector AS attrs
FROM pg_catalog.pg_publication_sequences s WHERE s.pubname IN (~a)
ORDER BY 1,2"
          publication-list publication-list))

(deftest discovery-catalogs-match-c-wire
    (with-reference-clients (client reference)
      (unwind-protect
           (progn
             (dolist (connection (list client reference))
               (pg::pg-client-query connection "CREATE TABLE catalog_wire_a (id int PRIMARY KEY,label varchar(20))")
               (pg::pg-client-query connection "CREATE TABLE catalog_wire_b (id int PRIMARY KEY)")
               (pg::pg-client-query connection "CREATE SEQUENCE catalog_wire_counter")
               (pg::pg-client-query connection "CREATE PUBLICATION catalog_wire_pub FOR TABLE catalog_wire_a,catalog_wire_b")
               (pg::pg-client-query connection "CREATE PUBLICATION catalog_wire_q FOR TABLE catalog_wire_a"))
             (dolist (sql (list
                           "SELECT oid,nspname FROM pg_namespace WHERE oid IN (11,2200) ORDER BY oid"
                           "SELECT oid FROM pg_namespace WHERE oid=11"
                           "SELECT * FROM pg_publication WHERE false"
                           "SELECT pubname,pubowner,puballtables,puballsequences,pubinsert,pubupdate,pubdelete,pubtruncate,pubviaroot,pubgencols FROM pg_publication WHERE pubname IN ('catalog_wire_pub','catalog_wire_q') ORDER BY pubname"
                           "SELECT 11::oid,'-1'::oid,'4294967295'::oid::int4,'4294967295'::oid::bigint,(-1::int4)::oid"
                           "SELECT (-1::bigint)::oid"
                           "SELECT true::oid"
                           "SELECT 1::oid+1"
                           "SELECT -(1::oid)"
                           "SELECT '0/1'::pg_lsn,'FFFFFFFF/FFFFFFFF'::pg_lsn"
                           "SELECT pg_current_wal_lsn() WHERE false"
                           "SELECT pg_current_wal_lsn()>='0/0'::pg_lsn"
                           "SELECT '0/1'::pg_lsn::text"
                           "SELECT 'F/FFFFFFFF'::pg_lsn<'10/0'::pg_lsn"
                           "SELECT '0/1'::pg_lsn IN ('0/2','0/1')"
                           "SELECT '0/1'::pg_lsn IN (1,2)"
                           "SELECT x FROM (SELECT '10/0'::pg_lsn AS x UNION ALL SELECT 'F/0'::pg_lsn) AS u ORDER BY x"
                           "SELECT ' 0/1'::pg_lsn"
                           "SELECT '+0/1'::pg_lsn"
                           "SELECT '0x1/1'::pg_lsn"
                           "SELECT '000000000/1'::pg_lsn"
                           "SELECT '0/1 '::pg_lsn"
                           "SELECT '١/1'::pg_lsn"
                           "SELECT oid FROM pg_namespace WHERE 11=oid"
                           "SELECT oid FROM pg_namespace WHERE oid IN (11)"
                           "SELECT oid FROM pg_namespace WHERE oid=11::bigint"
                           "SELECT oid FROM pg_namespace WHERE oid=-1"
                           "SELECT oid FROM pg_namespace WHERE oid=-1::bigint"
                           "SELECT oid FROM pg_namespace WHERE oid IN (11,-1)"
                           "SELECT oid FROM pg_namespace WHERE oid IN (11,-1::bigint)"
                           "SELECT relname,relkind,relnamespace FROM pg_class WHERE relname IN ('catalog_wire_a','catalog_wire_a_pkey','catalog_wire_b','catalog_wire_counter') ORDER BY relname"
                           "SELECT oid,relname,relnamespace,relkind FROM pg_class WHERE relname IN ('pg_class','pg_namespace','pg_publication','pg_publication_sequences','pg_replication_slots') ORDER BY oid"
                           "SELECT pubname,schemaname,sequencename FROM pg_publication_sequences WHERE pubname='catalog_wire_pub'"
                           "SELECT slot_name,active,restart_lsn,confirmed_flush_lsn,wal_status FROM pg_replication_slots WHERE false"
                           (native-publication-discovery-sql "'catalog_wire_pub','catalog_wire_q'")
                           (native-publication-discovery-sql "'catalog_wire_missing'")))
               (ok (equalp (observed-query-wire reference sql) (observed-query-wire client sql)) sql))
             (dolist (connection (list client reference))
               (pg::pg-client-query connection "ALTER PUBLICATION catalog_wire_pub SET TABLE catalog_wire_b"))
             (let ((sql (native-publication-discovery-sql "'catalog_wire_pub'")))
               (ok (equalp (observed-query-wire reference sql) (observed-query-wire client sql))))
             (dolist (connection (list client reference))
               (pg::pg-client-query connection "CREATE PUBLICATION catalog_wire_sequences FOR ALL SEQUENCES"))
             (let ((sql "SELECT pubname,puballtables,puballsequences FROM pg_publication WHERE pubname='catalog_wire_sequences'"))
               (ok (equalp (observed-query-wire reference sql) (observed-query-wire client sql))))
             (let ((sql "SELECT pubname,schemaname,sequencename FROM pg_publication_sequences WHERE pubname='catalog_wire_sequences' AND sequencename IN ('catalog_wire_counter','catalog_wire_later') ORDER BY sequencename"))
               (ok (equalp (observed-query-wire reference sql) (observed-query-wire client sql))))
             (dolist (connection (list client reference))
               (pg::pg-client-query connection "CREATE SEQUENCE catalog_wire_later"))
             (let ((sql "SELECT pubname,schemaname,sequencename FROM pg_publication_sequences WHERE pubname='catalog_wire_sequences' AND sequencename IN ('catalog_wire_counter','catalog_wire_later') ORDER BY sequencename"))
               (ok (equalp (observed-query-wire reference sql) (observed-query-wire client sql))))
             (dolist (connection (list client reference))
               (pg::pg-client-query connection "DROP SEQUENCE catalog_wire_counter"))
             (let ((sql "SELECT pubname,schemaname,sequencename FROM pg_publication_sequences WHERE pubname='catalog_wire_sequences' AND sequencename IN ('catalog_wire_counter','catalog_wire_later') ORDER BY sequencename"))
               (ok (equalp (observed-query-wire reference sql) (observed-query-wire client sql)))))
        (dolist (connection (list client reference))
          (dolist (name '("catalog_wire_sequences" "catalog_wire_pub" "catalog_wire_q"))
            (ignore-errors (pg::pg-client-query connection (format nil "DROP PUBLICATION ~a" name))))
          (dolist (name '("catalog_wire_later" "catalog_wire_counter"))
            (ignore-errors (pg::pg-client-query connection (format nil "DROP SEQUENCE ~a" name))))
          (dolist (name '("catalog_wire_b" "catalog_wire_a"))
            (ignore-errors (pg::pg-client-query connection (format nil "DROP TABLE ~a" name))))))))

(deftest casts-match-c-reference-wire
    (with-reference-clients (client reference)
      (dolist (sql '("SELECT '42'::int4, CAST('42' AS integer)"
                     "SELECT '0x_FF'::int4, '-0b_100'::int8"
                     "SELECT true::name, true::varchar, true::text"
                     "SELECT 'S'::\"char\", NULL::int2vector, '1 2 -3'::int2vector"
                     "SELECT 'é'::\"char\", ''::\"char\", (-128)::\"char\"::int4"
                     "SELECT 'abcdefgh'::varchar(3), 'x'::varchar(400)"
                     "SELECT '1'::text::int4, length('x')::text"
                     "SELECT 'x'::\"char\" IN ('x'::text, 'y'::text)"
                     "SELECT 'x'::\"char\" IN ('x'::\"char\", 'y'::text)"
                     "SELECT 1 IN ('1'::text, '2'::text)"
                     "SELECT 1 = '1'::text WHERE false"
                     "SELECT 'x'::\"char\" IN ('x'::\"char\", 'y'::\"char\")"
                     "SELECT '1 2'::int2vector IN ('1 2'::int2vector)"
                     "SELECT 'x'::\"char\"='x'::text, 'x'::\"char\"='x'::name"
                     "SELECT 'abc'::varchar(2) AS \"from\""
                     "SELECT 9223372036854775808::int8"
                     "SELECT (-9223372036854775808)::int8"
                     "SELECT '2147483648'::int4"
                     "SELECT '9223372036854775808'::int8"
                     "SELECT '0x__1'::int4"
                     "SELECT 'maybe'::bool"
                     "SELECT '32768'::int2vector"
                     "SELECT true::int8"
                     "SELECT 'x'::\"char\" > 0"
                     "SELECT '1 2'::int2vector='1 2'::text"
                     "SELECT 'invalid'::int4 WHERE false"
                     "SELECT CAST(\"true\" AS text)"
                     "SELECT length(VARIADIC 'x')"))
        (ok (equalp (observed-query-wire reference sql)
                    (observed-query-wire client sql))
            sql))))

(deftest smallint-casts-match-c-reference
    (with-reference-clients (client reference)
      (dolist (sql '("SELECT 32767::bigint::smallint, (-32768)::bigint::smallint"
                     "SELECT 9223372036854775807::bigint::smallint"
                     "SELECT (-9223372036854775808)::bigint::smallint"
                     "SELECT 32766.5::float8::smallint, (-32768.5)::float4::smallint"
                     "SELECT 32766.5::numeric::smallint"
                     "SELECT 32767.5::numeric::smallint"
                     "SELECT 1e30::numeric::smallint"
                     "SELECT 'NaN'::numeric::smallint"
                     "SELECT 'Infinity'::numeric::smallint"
                     "SELECT 'NaN'::float8::smallint"
                     "SELECT '-Infinity'::float4::smallint"))
        (ok (equalp (observed-query-wire reference sql)
                    (observed-query-wire client sql)) sql))))

(deftest array-aggregates-match-c-wire
    (with-reference-clients (client reference)
      (progn
        (dolist (connection (list client reference))
          (pg::pg-client-query connection "CREATE TABLE array_wire_rows (id int, label text)")
          (pg::pg-client-query connection "INSERT INTO array_wire_rows VALUES (1,'a'),(1,'a'),(2,NULL),(2,NULL)")
          (pg::pg-client-query connection "CREATE SEQUENCE array_wire_seq"))
        (dolist (sql '("SELECT DISTINCT COUNT(*) ORDER BY COUNT(*)"
                       "SELECT DISTINCT array_agg('x'::text) ORDER BY array_agg('x'::text)"
                       "SELECT DISTINCT array_agg('x'::text) ORDER BY pg_catalog.array_agg('x'::text)"
                       "SELECT 1 ORDER BY array_agg('x'::text)"
                       "SELECT COUNT(*) ORDER BY array_agg('x'::text)"
                       "SELECT DISTINCT 1 ORDER BY COUNT(*)"
                       "SELECT length(array_agg('x'::text))"
                       "SELECT array_agg('x'::text) + 1"
                       "SELECT array_agg('a,b'::text), array_agg('NULL'::text), array_agg(''::text), array_agg('é'::text)"
                       "SELECT array_agg('x'::text)"
                       "SELECT array_agg(NULL::text), COUNT(*)"
                       "SELECT array_agg(label::text), COUNT(*) FROM array_wire_rows WHERE false"
                       "SELECT CAST(COUNT(*) AS text), COUNT(*) + COUNT(*) FROM array_wire_rows"
                       "SELECT id+0 AS id,array_agg(label::text),COUNT(*) FROM array_wire_rows GROUP BY id ORDER BY id"
                       "SELECT array_agg(label::text)::text FROM array_wire_rows"
                       "SELECT length(array_agg(label::text)::text) FROM array_wire_rows"
                       "SELECT DISTINCT array_agg(label::text) FROM array_wire_rows GROUP BY id ORDER BY 1"
                       "SELECT COUNT(*) FROM array_wire_rows ORDER BY COUNT(*)"
                       "SELECT COUNT(*)::text FROM array_wire_rows ORDER BY COUNT(*)"
                       "SELECT array_agg(nextval('array_wire_seq')::text) FROM array_wire_rows"
                       "SELECT array_agg(nextval('array_wire_seq')::text),array_agg(nextval('array_wire_seq')::text) FROM array_wire_rows"
                       "SELECT currval('array_wire_seq')"
                       "SELECT array_agg('x')"
                       "SELECT array_agg(COUNT(*)::text)"))
          (ok (equalp (observed-query-wire reference sql)
                      (observed-query-wire client sql)) sql))
        (dolist (connection (list client reference))
          (pg::pg-client-query connection "DROP TABLE array_wire_rows")
          (pg::pg-client-query connection "DROP SEQUENCE array_wire_seq")))))

(deftest publication-srfs-match-c-wire
    (with-reference-clients (client reference)
      (progn
        (dolist (connection (list client reference))
          (pg::pg-client-query connection "CREATE TABLE srf_wire_a (id int PRIMARY KEY, label varchar(20))")
          (pg::pg-client-query connection "CREATE TABLE srf_wire_b (id int PRIMARY KEY)")
          (pg::pg-client-query connection "CREATE PUBLICATION srf_wire_p FOR TABLE srf_wire_a,srf_wire_b")
          (pg::pg-client-query connection "CREATE PUBLICATION srf_wire_q FOR TABLE srf_wire_a"))
        ;; OIDs are cluster-local. Compare the complete descriptor on an empty
        ;; projection and use identical scalar properties for populated rows.
        (dolist (sql '("SELECT (pg_get_publication_tables('srf_wire_p')).* WHERE false"
                       "SELECT (pg_get_publication_tables('srf_wire_p')).attrs, (pg_get_publication_tables('srf_wire_p')).qual"
                       "SELECT 9, (pg_get_publication_tables('srf_wire_p')).attrs LIMIT 1"
                       "SELECT (pg_get_publication_tables('srf_wire_p')).attrs, (pg_get_publication_tables('srf_wire_q')).attrs"
                       "SELECT (pg_get_publication_tables('srf_wire_p','srf_wire_p')).attrs"
                       "SELECT (pg_get_publication_tables()).*"
                       "SELECT (pg_get_publication_tables(VARIADIC NULL)).*"
                       "SELECT (pg_get_publication_tables(NULL)).*"
                       "SELECT (pg_get_publication_tables('srf_wire_missing')).*"
                       "SELECT (pg_get_publication_tables(VARIADIC array_agg(pubname::text))).attrs FROM pg_catalog.pg_publication WHERE pubname IN ('srf_wire_p')"
                       "SELECT (pg_get_publication_tables(VARIADIC array_agg(pubname::text))).* FROM pg_catalog.pg_publication WHERE pubname IN ('srf_wire_missing')"
                       "SELECT array_agg(pg_get_publication_tables('srf_wire_p'))"
                       "SELECT 1 WHERE pg_get_publication_tables('srf_wire_p') IS NULL"))
          (ok (equalp (observed-query-wire reference sql) (observed-query-wire client sql)) sql))
        (dolist (connection (list client reference))
          (let ((before (pg::pg-client-query connection "SELECT oid FROM pg_catalog.pg_publication WHERE pubname = 'srf_wire_p'")))
            (pg::pg-client-query connection "ALTER PUBLICATION srf_wire_p SET (publish = 'insert')")
            (ok (equalp before (pg::pg-client-query connection "SELECT oid FROM pg_catalog.pg_publication WHERE pubname = 'srf_wire_p'")))
            (pg::pg-client-query connection "DROP PUBLICATION srf_wire_p")
            (pg::pg-client-query connection "CREATE PUBLICATION srf_wire_p FOR TABLE srf_wire_a")
            (ok (not (equalp before (pg::pg-client-query connection "SELECT oid FROM pg_catalog.pg_publication WHERE pubname = 'srf_wire_p'")))))
          (pg::pg-client-query connection "DROP PUBLICATION srf_wire_p")
          (pg::pg-client-query connection "DROP PUBLICATION srf_wire_q")
          (pg::pg-client-query connection "DROP TABLE srf_wire_a")
          (pg::pg-client-query connection "DROP TABLE srf_wire_b")))))

(deftest publication-srf-sharing-matches-c
    (with-reference-clients (client reference)
      (dolist (connection (list client reference))
        (unwind-protect
             (progn
               (pg::pg-client-query connection "CREATE TABLE srf_volatile_a (id int PRIMARY KEY)")
               (pg::pg-client-query connection "CREATE SEQUENCE srf_field_calls")
               (loop for index from 1 to 4 do
                 (pg::pg-client-query connection
                                      (format nil "CREATE PUBLICATION ~c~a~c FOR TABLE srf_volatile_a"
                                              #\" index #\")))
               (dolist (case
                           '(("SELECT (pg_get_publication_tables(nextval('srf_field_calls')::text)).* ORDER BY relid" "1")
                             ("SELECT pg_get_publication_tables(nextval('srf_field_calls')::text), pg_get_publication_tables(nextval('srf_field_calls')::text)" "2")
                             ("SELECT pg_get_publication_tables(nextval('srf_field_calls')::text), (pg_get_publication_tables(nextval('srf_field_calls')::text)).pubid" "1")
                             ("SELECT (pg_get_publication_tables(VARIADIC array_agg(nextval('srf_field_calls')::text))).* FROM (SELECT 1) AS input" "4")))
                 (pg::pg-client-query connection "SELECT setval('srf_field_calls',1,false)")
                 (ok (= 1 (length (pg::pg-client-query connection (first case)))) (first case))
                 (ok (equalp (list (vector (second case)))
                             (pg::pg-client-query connection "SELECT currval('srf_field_calls')"))
                     (first case))))
          (loop for index from 1 to 4 do
            (ignore-errors
             (pg::pg-client-query connection (format nil "DROP PUBLICATION ~c~a~c" #\" index #\"))))
          (ignore-errors (pg::pg-client-query connection "DROP SEQUENCE srf_field_calls"))
          (ignore-errors (pg::pg-client-query connection "DROP TABLE srf_volatile_a"))))))

(deftest union-all-matches-c-wire
    (with-reference-clients (client reference)
      (progn
        (dolist (sql '("SELECT 1 AS x UNION ALL SELECT 1"
                       "SELECT 3 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY x DESC LIMIT 2"
                       "(SELECT 3 ORDER BY 1 LIMIT 1) UNION ALL SELECT 1"
                       "(SELECT 3 ORDER BY 1 LIMIT 0) UNION ALL SELECT 1"
                       "SELECT 3 UNION ALL (SELECT 1 UNION ALL SELECT 2 ORDER BY 1 LIMIT 1) ORDER BY 1"
                       "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2) AS u ORDER BY x"
                       "SELECT count(*) FROM (SELECT 1 UNION ALL SELECT 1) AS u"
                       "SELECT 1 WHERE false UNION ALL SELECT 2"
                       "SELECT 1 UNION ALL SELECT 2 LIMIT 0"
                       "SELECT '1' UNION ALL SELECT 2"
                       "SELECT NULL UNION ALL SELECT 2"
                       "SELECT NULL UNION ALL SELECT NULL"
                       "SELECT 1 UNION ALL SELECT 2147483648"
                       "SELECT 'abc'::varchar(3) UNION ALL SELECT 'def'::varchar(3)"
                       "SELECT 'abc'::varchar(3) UNION ALL SELECT 'def'::varchar(4)"
                       "SELECT 'abc'::varchar UNION ALL SELECT 'def'::text"
                       "SELECT 'abc'::text UNION ALL SELECT 'def'::varchar"
                       "SELECT 'abc'::name UNION ALL SELECT 'def'::varchar"
                       "SELECT 'abc'::varchar UNION ALL SELECT 'def'::name"
                       "SELECT NULL::int2vector UNION ALL SELECT '1 2'::int2vector"
                       "SELECT pubname FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT 'x'::name"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT -1"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT -1::bigint"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT 4294967296"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT '0x10'"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT '010'"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT '0b101'"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT '0B1'"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT '0b2'"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT '09'"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT '١'"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT '0x١'"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT '-1'"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT '-18446744073709551615'"
                       "SELECT oid FROM pg_catalog.pg_publication WHERE false UNION ALL SELECT '18446744073709551616x'"
                       "SELECT 1 UNION ALL SELECT 1,2"
                       "SELECT 1 UNION ALL SELECT true"
                       "SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT 1"
                       "SELECT 'bad' UNION ALL SELECT 1"
                       "SELECT 1 AS x UNION ALL SELECT 2 AS y ORDER BY y"
                       "SELECT 1 AS x UNION ALL SELECT 2 ORDER BY x+1"
                       "SELECT 1 AS x UNION ALL SELECT 2 ORDER BY t.x"
                       "SELECT 1 AS x,2 AS x UNION ALL SELECT 2,3 ORDER BY x"
                       "SELECT 1 UNION ALL SELECT 2 ORDER BY 3"
                       "SELECT 1 UNION ALL SELECT 2 ORDER BY 'x'"
                       "(SELECT 1 LIMIT 1) LIMIT 2"))
          (ok (equalp (observed-query-wire reference sql) (observed-query-wire client sql)) sql))
        (dolist (connection (list client reference))
          (unwind-protect
               (progn
                 (pg::pg-client-query connection "CREATE SEQUENCE union_call_counter")
                 (dolist (case '(("SELECT nextval('union_call_counter') UNION ALL SELECT nextval('union_call_counter') LIMIT 1" "2")
                                 ("(SELECT nextval('union_call_counter') LIMIT 1) UNION ALL SELECT nextval('union_call_counter')" "3")
                                 ("SELECT nextval('union_call_counter') UNION ALL SELECT nextval('union_call_counter') LIMIT 0" "1")))
                   (pg::pg-client-query connection "SELECT setval('union_call_counter',1,false)")
                   (pg::pg-client-query connection (first case))
                   (ok (equalp (list (vector (second case)))
                               (pg::pg-client-query connection "SELECT nextval('union_call_counter')"))
                       (first case))))
            (ignore-errors (pg::pg-client-query connection "DROP SEQUENCE union_call_counter")))))))

(deftest union-all-indexed-arm-domains
    (with-reference-clients (client reference)
      (unwind-protect
           (progn
             (dolist (connection (list client reference))
               (pg::pg-client-query connection "CREATE TABLE union_arm_a (id int PRIMARY KEY,v varchar(126))")
               (pg::pg-client-query connection "CREATE TABLE union_arm_b (id bigint PRIMARY KEY,v varchar(126))")
               (pg::pg-client-query connection "INSERT INTO union_arm_a VALUES (1,'a'),(2,'b'),(3,'c')")
               (pg::pg-client-query connection "INSERT INTO union_arm_b VALUES (2,'z'),(4,'d')")
               ;; Make both heap scans expensive enough for the index path to win.
               (dolist (table '("union_arm_a" "union_arm_b"))
                 (pg::pg-client-query connection
                                      (format nil "INSERT INTO ~a VALUES ~{~a~^,~}" table
                                              (loop for id from 100 to 699
                                                    collect (format nil "(~a,'~a')" id
                                                                    (make-string 126 :initial-element #\x)))))))
             (dolist (sql '("SELECT id,v FROM union_arm_a WHERE id=1 UNION ALL SELECT id,v FROM union_arm_b WHERE id=2 ORDER BY 1"
                            "SELECT id,v FROM union_arm_a UNION ALL SELECT id,v FROM union_arm_b ORDER BY id,v"
                            "SELECT DISTINCT id FROM (SELECT id FROM union_arm_a UNION ALL SELECT id FROM union_arm_b) AS u ORDER BY id"
                            "SELECT u.id,u.v FROM (SELECT id,v FROM union_arm_a UNION ALL SELECT id,v FROM union_arm_b) AS u JOIN union_arm_a AS a ON a.id=u.id ORDER BY u.id,u.v"
                            "(SELECT id,v FROM union_arm_a ORDER BY id DESC LIMIT 1) UNION ALL (SELECT id,v FROM union_arm_b ORDER BY id LIMIT 1) ORDER BY id"))
               (ok (equalp (observed-query-wire reference sql) (observed-query-wire client sql)) sql))
             (let ((plan (format nil "~{~a~%~}"
                                 (mapcar (lambda (row) (aref row 0))
                                         (pg::pg-client-query client
                                                              "EXPLAIN SELECT id FROM union_arm_a WHERE id=1 UNION ALL SELECT id FROM union_arm_b WHERE id=2")))))
               (ok (search "Append" plan))
               (ok (= 2 (loop for start = 0 then (+ position 10)
                              for position = (search "Index Scan" plan :start2 start)
                              while position count position)))))
        (dolist (connection (list client reference))
          (ignore-errors (pg::pg-client-query connection "DROP TABLE union_arm_b"))
          (ignore-errors (pg::pg-client-query connection "DROP TABLE union_arm_a"))))))

(defun observed-column-origin (client query)
  "Read one result column's origin and type directly from its wire descriptor."
  (pg::pg-client-send-query client query)
  (let ((origin nil))
    (loop
      (multiple-value-bind (kind payload) (pg::pg-client-frame (pg::pg-client-stream client))
        (case kind
          (#\T
           (unless (= 1 (pg::bytes-to-u16 payload 0)) (error "expected one column"))
           (multiple-value-bind (name offset) (pg::pg-client-cstring payload 2)
             (setf origin (list name (pg::bytes-to-u32 payload offset)
                                (pg::bytes-to-u16 payload (+ offset 4))
                                (pg::bytes-to-u32 payload (+ offset 6))))))
          (#\E (error (pg::pg-client-error-from-payload payload)))
          (#\Z (return origin))
          ((#\D #\C #\S #\N) nil)
          (otherwise (error "unexpected backend response ~s" kind)))))))

(deftest native-publication-lookup-uses-catalog-state
    (let* ((port (parse-integer (or (uiop:getenv "PG_INTEROP_TARGET_PORT")
                                    (error "isolated target port required"))))
           (client (pg::pg-client-connect "127.0.0.1" port "postgres" :replication-p t)))
      (unwind-protect
           (progn
             (pg::pg-client-query client "CREATE TABLE discovery_rows (id integer PRIMARY KEY)")
             (pg::pg-client-query client "INSERT INTO discovery_rows VALUES (1)")
             (pg::pg-client-query
              client "CREATE PUBLICATION discovery_pub FOR TABLE discovery_rows WITH (publish = 'insert, delete')")
             (observed-query-status client "SELECT pg_catalog.set_config('search_path', '', false)")
             ;; Exact first query shape from C check_publications(), including alias.
             (multiple-value-bind (rows columns)
                 (pg::pg-client-query
                  client "SELECT t.pubname FROM pg_catalog.pg_publication t WHERE t.pubname IN ('missing_pub', 'discovery_pub')")
               (ok (equalp '(#("discovery_pub")) rows))
               (ok (= 19 (pg::pg-client-column-type-oid (first columns)))))
             (ok (equalp '(#("t" "f" "t" "f"))
                         (pg::pg-client-query
                          client "SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_catalog.pg_publication WHERE pubname = 'discovery_pub'")))
             (pg::pg-client-query client "ALTER PUBLICATION discovery_pub SET (publish = 'update')")
             (ok (equalp '(#("f" "t" "f" "f"))
                         (pg::pg-client-query
                          client "SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_catalog.pg_publication WHERE pubname = 'discovery_pub'")))
             (ok (equal '("pubname" 6104 2 19)
                        (observed-column-origin
                         client "SELECT pubname FROM pg_catalog.pg_publication")))
             (ok (equalp '(#("t" "f"))
                         (pg::pg-client-query
                          client "SELECT id IN (1, id / 0), id NOT IN (1, id / 0) FROM public.discovery_rows")))
             (pg::pg-client-query client "DROP PUBLICATION discovery_pub")
             (ok (null (pg::pg-client-query
                        client "SELECT pubname FROM pg_catalog.pg_publication WHERE pubname IN ('discovery_pub')"))))
        (pg::pg-client-close client))))

(deftest distinct-live-projection-order-and-limit
    (let* ((port (parse-integer (or (uiop:getenv "PG_INTEROP_TARGET_PORT")
                                    (error "isolated target port required"))))
           (client (pg::pg-client-connect "127.0.0.1" port "postgres")))
      (unwind-protect
           (progn
             (pg::pg-client-query client "CREATE TABLE distinct_live_rows (a int, b text)")
             (pg::pg-client-query client "INSERT INTO distinct_live_rows VALUES (1,'a'),(1,'a'),(1,NULL),(1,NULL),(2,'b'),(3,'c')")
             (ok (equalp '(#("3") #("2"))
                         (pg::pg-client-query client "SELECT DISTINCT a AS value FROM distinct_live_rows ORDER BY value DESC LIMIT 2")))
             (ok (equalp '(#("1" "a") #("1" nil) #("2" "b") #("3" "c"))
                         (pg::pg-client-query client "SELECT DISTINCT * FROM distinct_live_rows ORDER BY a,b")))
             (ok (equalp '(#("1" "4") #("2" "1") #("3" "1"))
                         (pg::pg-client-query client "SELECT DISTINCT a, COUNT(*) FROM distinct_live_rows GROUP BY a ORDER BY a")))
             (ok (handler-case
                     (progn (pg::pg-client-query client "SELECT DISTINCT a FROM distinct_live_rows ORDER BY b") nil)
                   (pg::pg-client-error (condition)
                     (string= "42P10" (pg::pg-client-error-sqlstate condition)))))
             (pg::pg-client-query client "DROP TABLE distinct_live_rows"))
        (pg::pg-client-close client))))

(deftest derived-live-indexes-and-origins
    (let* ((port (parse-integer (or (uiop:getenv "PG_INTEROP_TARGET_PORT")
                                    (error "isolated target port required"))))
           (client (pg::pg-client-connect "127.0.0.1" port "postgres")))
      (unwind-protect
           (progn
             (pg::pg-client-query client "CREATE TABLE derived_live_rows (id int PRIMARY KEY, v int)")
             (pg::pg-client-query
              client (format nil "INSERT INTO derived_live_rows VALUES ~{~a~^,~}"
                             (loop for i below 1000 collect (format nil "(~d,~d)" i (* i 10)))))
             (pg::pg-client-query client "ANALYZE derived_live_rows")
             (ok (equalp '(#("2" "20"))
                         (pg::pg-client-query client "SELECT q.id,q.v FROM (SELECT id,v FROM derived_live_rows WHERE id=2) q")))
             (ok (equalp '(#("2"))
                         (pg::pg-client-query client "SELECT d.id FROM (SELECT a.id FROM derived_live_rows a JOIN derived_live_rows b ON a.id=b.id WHERE b.id=2) d")))
             (let ((plan (aref (first (pg::pg-client-query client "EXPLAIN ANALYZE SELECT d.id FROM (SELECT a.id FROM derived_live_rows a JOIN derived_live_rows b ON a.id=b.id WHERE b.id=2) d")) 0)))
               (ok (search "Subquery Scan on d" plan))
               (ok (search "Index Scan using derived_live_rows_pkey" plan)))
             (ok (equal (observed-column-origin client "SELECT id FROM derived_live_rows")
                        (observed-column-origin client "SELECT q.id FROM (SELECT id FROM derived_live_rows) q")))
             (ok (equal '("pubname" 6104 2 19)
                        (observed-column-origin client "SELECT q.pubname FROM (SELECT pubname FROM pg_catalog.pg_publication) q")))
             (pg::pg-client-query client "DROP TABLE derived_live_rows"))
        (pg::pg-client-close client))))

(deftest public-table-cannot-be-confused-with-a-catalog
    (let* ((port (parse-integer (or (uiop:getenv "PG_INTEROP_TARGET_PORT")
                                    (error "isolated target port required"))))
           (client (pg::pg-client-connect "127.0.0.1" port "postgres")))
      (unwind-protect
           (progn
             (pg::pg-client-query client "CREATE TABLE pg_replication_slots (id integer PRIMARY KEY)")
             (observed-query-status client "SET search_path TO public, pg_catalog")
             (pg::pg-client-query client "INSERT INTO pg_replication_slots VALUES (9)")
             (observed-query-status client "RESET search_path")
             (ok (equal '("0A000" #\I)
                        (observed-query-status client "DROP TABLE pg_replication_slots")))
             (ok (equal '("0A000" #\I)
                        (observed-query-status client "ALTER TABLE pg_replication_slots ADD UNIQUE (id)")))
             (ok (equalp '(#("9"))
                         (pg::pg-client-query client "SELECT id FROM public.pg_replication_slots")))
             (ok (equal '("0A000" #\I)
                        (observed-query-status client "DROP TABLE \"public.pg_replication_slots\"")))
             (ok (equalp '(#("9"))
                         (pg::pg-client-query client "SELECT id FROM public.pg_replication_slots"))))
        (ignore-errors (observed-query-status client "SET search_path TO public, pg_catalog"))
        (ignore-errors (pg::pg-client-query client "DROP TABLE pg_replication_slots"))
        (pg::pg-client-close client))))

(deftest startup-reports-search-path
    (let* ((port (parse-integer (or (uiop:getenv "PG_INTEROP_TARGET_PORT")
                                    (error "isolated target port required"))))
           (socket (make-instance 'sb-bsd-sockets:inet-socket :type :stream :protocol :tcp))
           (stream nil)
           (parameters nil))
      (unwind-protect
           (progn
             (sb-bsd-sockets:socket-connect socket #(127 0 0 1) port)
             (setf stream (sb-bsd-sockets:socket-make-stream
                           socket :input t :output t :element-type '(unsigned-byte 8)
                           :buffering :none))
             (pg::pg-client-send-startup stream "postgres" "postgres" "interop-startup" nil)
             (loop
               (multiple-value-bind (kind payload) (pg::pg-client-frame stream)
                 (case kind
                   (#\S
                    (multiple-value-bind (name offset) (pg::pg-client-cstring payload 0)
                      (push (cons name (pg::pg-client-cstring payload offset)) parameters)))
                   (#\Z (return))
                   ((#\R #\K #\N) nil)
                   (otherwise (error "unexpected startup response ~s" kind)))))
             (ok (equal '("search_path" . "\"$user\", public")
                        (assoc "search_path" parameters :test #'string=))))
        (when stream (close stream))
        (ignore-errors (sb-bsd-sockets:socket-close socket)))))

(deftest search-path-session-and-undo
    (let* ((port (parse-integer (or (uiop:getenv "PG_INTEROP_TARGET_PORT")
                                    (error "isolated target port required"))))
           (client (pg::pg-client-connect "127.0.0.1" port "postgres" :replication-p t))
           (observer (pg::pg-client-connect "127.0.0.1" port "postgres")))
      (unwind-protect
           (progn
             (pg::pg-client-query client "CREATE TABLE path_rows (id integer PRIMARY KEY)")
             (pg::pg-client-query client "INSERT INTO path_rows VALUES (7)")
             (multiple-value-bind (status rows parameters)
                 (observed-query-status
                  client "SELECT pg_catalog.set_config('search_path', '', false)")
               (ok (equal '(nil #\I) status))
               (ok (equalp '(#("")) rows))
               (ok (equal '("search_path" . "")
                          (assoc "search_path" parameters :test #'string=))))
             (ok (equalp '(#("")) (pg::pg-client-query client "SHOW search_path")))
             (ok (equalp '(#(nil)) (pg::pg-client-query client "SELECT current_schema()")))
             (ok (equal '("42P01" #\I)
                        (observed-query-status client "SELECT * FROM path_rows")))
             (ok (equalp '(#("7"))
                         (pg::pg-client-query client "SELECT path_rows.id FROM public.path_rows")))
             (ok (equalp '(#("0"))
                         (pg::pg-client-query client "SELECT count(*) FROM pg_catalog.pg_replication_slots")))
             (ok (equalp '(#("\"$user\", public"))
                         (pg::pg-client-query observer "SHOW search_path")))
             (ok (equal '("3F000" #\I)
                        (observed-query-status client "CREATE TABLE path_forbidden (id integer)")))
             (pg::pg-client-query client "BEGIN")
             (observed-query-status client "SELECT set_config('search_path', 'public', false)")
             (ok (equalp '(#("7")) (pg::pg-client-query client "SELECT * FROM path_rows")))
             (observed-query-status client "ROLLBACK")
             (ok (equalp '(#("")) (pg::pg-client-query client "SHOW search_path")))
             (ok (equal '("22012" #\I)
                        (observed-query-status
                         client "SELECT set_config('search_path', 'public', false), 1/0")))
             (ok (equalp '(#("")) (pg::pg-client-query client "SHOW search_path")))
             (pg::pg-client-query client "BEGIN")
             (observed-query-status client "SET search_path = 'public'")
             (pg::pg-client-query client "COMMIT")
             (ok (equalp '(#("public")) (pg::pg-client-query client "SHOW search_path")))
             (observed-query-status client "RESET search_path")
             (ok (equalp '(#("\"$user\", public"))
                         (pg::pg-client-query client "SHOW search_path"))))
        (pg::pg-client-close client)
        (pg::pg-client-close observer))))

(deftest slot-command-success-preserves-transaction-state
    (let* ((port (parse-integer
                  (or (uiop:getenv "PG_INTEROP_TARGET_PORT")
                      (error "PG_INTEROP_TARGET_PORT must name an isolated test target"))))
           (client (pg::pg-client-connect "127.0.0.1" port "postgres" :replication-p t)))
      (unwind-protect
           (progn
             (ok (equal '(nil #\I)
                        (observed-query-status
                         client "CREATE_REPLICATION_SLOT context_success_slot LOGICAL pg_lisp EXPORT_SNAPSHOT")))
             (ok (equal '(nil #\T) (observed-query-status client "BEGIN")))
             (ok (equal '(nil #\T)
                        (observed-query-status client "DROP_REPLICATION_SLOT context_success_slot")))
             (ok (equal '(nil #\I) (observed-query-status client "COMMIT")))
             (ok (equal '(nil #\T) (observed-query-status client "BEGIN")))
             (ok (equal '("25001" #\E)
                        (observed-query-status
                         client "CREATE_REPLICATION_SLOT context_forbidden_slot LOGICAL pg_lisp EXPORT_SNAPSHOT")))
             (ok (equal '(nil #\I) (observed-query-status client "ROLLBACK")))
             (ok (equalp '(#("0"))
                         (pg::pg-client-query
                          client "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'context_forbidden_slot'"))))
        (ignore-errors (observed-query-status client "ROLLBACK"))
        (ignore-errors (observed-query-status client "DROP_REPLICATION_SLOT context_success_slot"))
        (pg::pg-client-close client))))

(deftest identify-system-matches-c-wire
    (let* ((target-port
             (parse-integer
              (or (uiop/os:getenv "PG_INTEROP_TARGET_PORT")
                  (error "isolated target port is required"))))
           (reference-port
             (parse-integer
              (or (uiop/os:getenv "PG_INTEROP_REFERENCE_PORT")
                  (error "reference port is required"))))
           (target (pg::pg-client-connect "127.0.0.1" target-port "postgres" :replication-p t))
           (reference
             (pg::pg-client-connect "127.0.0.1" reference-port "postgres" :user
                                    (or (uiop/os:getenv "PG_INTEROP_REFERENCE_USER") "postgres")
                                    :replication-p t)))
      (unwind-protect
           (let* ((private-before
                    (pg::pg-client-query target "SELECT pg_lisp_cluster_identity()"))
                  (actual (observed-query-wire target "IDENTIFY_SYSTEM"))
                  (expected (observed-query-wire reference "IDENTIFY_SYSTEM"))
                  (row (first (pg::pg-client-query target "IDENTIFY_SYSTEM"))))
             (ok (eq :ok (first actual)))
             (ok (eq :ok (first expected)))
             (dolist (kind '(#\T #\C #\Z))
               (ok (equalp (assoc kind (rest actual)) (assoc kind (rest expected)))))
             (ok (<= 0 (parse-integer (aref row 0)) 18446744073709551615))
             (ok (equal "1" (aref row 1)))
             (ok (integerp (pg::parse-replication-lsn (aref row 2))))
             (ok (equal "postgres" (aref row 3)))
             (ok
              (equalp private-before
                      (pg::pg-client-query target "SELECT pg_lisp_cluster_identity()"))))
        (pg::pg-client-close target)
        (pg::pg-client-close reference))))

(deftest standard-slot-wire-snapshot-ownership
    (let* ((port
             (parse-integer
              (or (uiop/os:getenv "PG_INTEROP_TARGET_PORT")
                  (error "PG_INTEROP_TARGET_PORT must name an isolated test target"))))
           (client (pg::pg-client-connect "127.0.0.1" port "postgres" :replication-p t))
           (observer (pg::pg-client-connect "127.0.0.1" port "postgres")))
      (unwind-protect
           (progn
             (pg::pg-client-query observer
                                  "CREATE TABLE wire_native_rows (id integer PRIMARY KEY, amount integer)")
             (pg::pg-client-query observer "INSERT INTO wire_native_rows VALUES (1, 10)")
             (ok
              (equal '(nil #\T)
                     (observed-query-status client
                                            "BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")))
             (let ((rows
                     (pg::pg-client-query client
                                          "CREATE_REPLICATION_SLOT wire_native_nothing LOGICAL pgoutput (SNAPSHOT 'nothing', TWO_PHASE false, FAILOVER false)")))
               (ok (= 1 (length rows)))
               (ok (null (aref (first rows) 2)))
               (ok (equal "pgoutput" (aref (first rows) 3))))
             (ok
              (equal '(nil #\T)
                     (observed-query-status client
                                            "CREATE_REPLICATION_SLOT wire_native_slot LOGICAL pgoutput (SNAPSHOT 'use')")))
             (pg::pg-client-query observer "UPDATE wire_native_rows SET amount = 20 WHERE id = 1")
             (ok
              (equalp '(#("10"))
                      (pg::pg-client-query client "SELECT amount FROM wire_native_rows")))
             (ok
              (equal '(nil #\T)
                     (observed-query-status client "DROP_REPLICATION_SLOT \"wire_native_slot\"")))
             (ok
              (equalp '(#("10"))
                      (pg::pg-client-query client "SELECT amount FROM wire_native_rows")))
             (ok (equal '(nil #\I) (observed-query-status client "COMMIT")))
             (ok
              (equalp '(#("20"))
                      (pg::pg-client-query client "SELECT amount FROM wire_native_rows")))
             (ok
              (equal '(nil #\T)
                     (observed-query-status client
                                            "BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")))
             (pg::pg-client-query client "SELECT 1")
             (ok
              (equal '("XX000" #\E)
                     (observed-query-status client
                                            "CREATE_REPLICATION_SLOT wire_native_slot LOGICAL pgoutput (SNAPSHOT 'use')")))
             (ok (equal '(nil #\I) (observed-query-status client "ROLLBACK")))
             (ok
              (equal '("XX000" #\I)
                     (observed-query-status client
                                            "CREATE_REPLICATION_SLOT wire_native_slot LOGICAL pgoutput (SNAPSHOT 'use')")))
             (ok (equal '(nil #\T) (observed-query-status client "BEGIN READ ONLY")))
             (ok
              (equal '("XX000" #\E)
                     (observed-query-status client
                                            "CREATE_REPLICATION_SLOT wire_native_slot LOGICAL pgoutput (SNAPSHOT 'use')")))
             (ok (equal '(nil #\I) (observed-query-status client "ROLLBACK")))
             (ok
              (equal '(nil #\T)
                     (observed-query-status client "BEGIN ISOLATION LEVEL REPEATABLE READ")))
             (ok
              (equal '("XX000" #\E)
                     (observed-query-status client
                                            "CREATE_REPLICATION_SLOT wire_native_slot LOGICAL pgoutput (SNAPSHOT 'use')")))
             (ok (equal '(nil #\I) (observed-query-status client "ROLLBACK")))
             (dolist (name '("wire_native_nothing" "wire_native_slot"))
               (ignore-errors
                (observed-query-status client (format nil "DROP_REPLICATION_SLOT ~a" name)))))
        (ignore-errors (observed-query-status client "ROLLBACK"))
        (dolist (name '("wire_native_nothing" "wire_native_slot"))
          (ignore-errors
           (observed-query-status client (format nil "DROP_REPLICATION_SLOT ~a" name))))
        (pg::pg-client-close client)
        (ignore-errors (pg::pg-client-query observer "DROP TABLE wire_native_rows"))
        (pg::pg-client-close observer))))

(defun observed-query-status (client query)
  "Observe status, rows, and ParameterStatus independently of the worker client."
  (pg::pg-client-send-query client query)
  (let ((sqlstate nil)
        (columns nil)
        (rows nil)
        (parameters nil))
    (loop
      (multiple-value-bind (kind payload) (pg::pg-client-frame (pg::pg-client-stream client))
        (case kind
          (#\E
           (setf sqlstate (pg::pg-client-error-sqlstate
                           (pg::pg-client-error-from-payload payload))))
          (#\T (setf columns (pg::pg-client-read-column-description payload)))
          (#\D (push (pg::pg-client-read-data-row payload columns) rows))
          (#\S
           (multiple-value-bind (name offset) (pg::pg-client-cstring payload 0)
             (push (cons name (pg::pg-client-cstring payload offset)) parameters)))
          (#\Z (return (values (list sqlstate (code-char (aref payload 0)))
                               (nreverse rows) (nreverse parameters))))
          ((#\C #\N) nil)
          (otherwise (error "unexpected backend response ~s" kind)))))))

(deftest replication-errors-preserve-transaction-state
    (let* ((port (parse-integer
                  (or (uiop:getenv "PG_INTEROP_TARGET_PORT")
                      (error "PG_INTEROP_TARGET_PORT must name an isolated test target"))))
           (client (pg::pg-client-connect "127.0.0.1" port "postgres" :replication-p t)))
      (unwind-protect
           (progn
             (ok (equal '(nil #\T) (observed-query-status client "BEGIN")))
             (ok (equal '("25001" #\E)
                        (observed-query-status
                         client "START_REPLICATION SLOT absent_slot LOGICAL 0/0 (proto_version '2')")))
             (ok (equal '("25P02" #\E)
                        (observed-query-status client "DROP_REPLICATION_SLOT absent_slot")))
             (ok (equal '("25P02" #\E) (observed-query-status client "SELECT 1")))
             (ok (equal '(nil #\I) (observed-query-status client "ROLLBACK")))
             (ok (equalp '(#("2")) (pg::pg-client-query client "SELECT 2"))))
        (pg::pg-client-close client))))

(deftest logical-connection-allows-sql
    (let* ((port (parse-integer
                  (or (uiop:getenv "PG_INTEROP_TARGET_PORT")
                      (error "PG_INTEROP_TARGET_PORT must name an isolated test target"))))
           (client (pg::pg-client-connect "127.0.0.1" port "postgres" :replication-p t)))
      (unwind-protect
           (progn
             (ok (equalp '(#("1")) (pg::pg-client-query client "SELECT 1")))
             (ok (equalp '(#("PostgreSQL 20devel (pg-lisp 0.1)"))
                         (pg::pg-client-query client "SELECT pg_catalog.version()")))
             (ok (equalp '(#("20devel"))
                         (pg::pg-client-query client "SHOW server_version")))
             (pg::pg-client-query client "BEGIN")
             (ok (equalp '(#("postgres"))
                         (pg::pg-client-query client "SELECT current_database()")))
             (pg::pg-client-query client "COMMIT")
             (ok (handler-case
                     (progn (pg::pg-client-query client "SELECT public.version()") nil)
                   (pg::pg-client-error (condition)
                     (string= (pg::pg-client-error-sqlstate condition) "42883"))))
             (ok (equalp '(#("2")) (pg::pg-client-query client "SELECT 2"))))
        (pg::pg-client-close client))))

(deftest captured-c-transactions
    ;; This is a dispatcher integration test, not native subscriber acceptance.
    ;; The caller supplies a fresh task-owned target, never an existing database.
    (let* ((port (parse-integer
                  (or (uiop:getenv "PG_INTEROP_TARGET_PORT")
                      (error "PG_INTEROP_TARGET_PORT must name an isolated test target"))))
           (options (pg::make-logical-subscription-options
                     :target-port port :slot-name "fixture_apply" :tables '("fixture_rows")))
           (target (pg::logical-startup-target-client options))
           (observer (pg::logical-startup-target-client options))
           (frames (pg-interop-tests::reference-frames))
           (commits 0)
           (expected '(#(#("1" "2")) #(#("1" "3")) #(#("4" "3")) #()
                       #(#("5" "6")) #())))
      (unwind-protect
           (with-open-file (feedback "/dev/null" :direction :output
                                                 :element-type '(unsigned-byte 8)
                                                 :if-exists :overwrite)
             (let ((runtime (pg::make-logical-subscription-runtime
                             :options options :target target
                             :source-client (pg::make-pg-client :stream feedback)
                             :current-lsn 0)))
               (pg::logical-target-query
                target "CREATE TABLE fixture_rows (id integer PRIMARY KEY, amount integer)")
               (pg::logical-ensure-progress-table target)
               ;; Replay six original C transactions through Commit. The following
               ;; private Message fixtures are separately covered by codec tests.
               (loop for frame in frames
                     for kind = (code-char (aref frame 0))
                     until (= commits 6)
                     do (when (char= kind #\C)
                          ;; Another connection must still see the preceding commit.
                          (ok (equalp
                               (coerce (pg::logical-target-query
                                        observer "SELECT id, amount FROM fixture_rows ORDER BY id")
                                       'vector)
                               (if (zerop commits) #() (nth (1- commits) expected)))))
                        (pg::logical-pgoutput-dispatch runtime frame)
                        (when (char= kind #\C)
                          (ok (equalp
                               (coerce (pg::logical-target-query
                                        observer "SELECT id, amount FROM fixture_rows ORDER BY id")
                                       'vector)
                               (nth commits expected)))
                          (ok (string=
                               (getf (pg::logical-read-progress observer "fixture_apply") :lsn)
                               (pg::wal-format-lsn (pg::pg-client-u64 frame 10))))
                          (incf commits)))
               (ok (= commits 6))
               ;; A synthetic multi-relation native T exercises grouped dispatch,
               ;; followed by rollback to check transactional visibility.
               (pg::logical-target-query
                target "CREATE TABLE fixture_other (id integer PRIMARY KEY, amount integer)")
               (pg::logical-target-query
                target "INSERT INTO fixture_rows VALUES (8, 8); INSERT INTO fixture_other VALUES (9, 9)")
               (setf (pg::logical-subscription-options-tables options)
                     '("fixture_rows" "fixture_other"))
               (let* ((base (gethash 24587 (pg::logical-subscription-runtime-relations runtime)))
                      (other (pg::copy-pgoutput-relation base)))
                 (setf (pg::pgoutput-relation-name other) "fixture_other"
                       (pg::pgoutput-relation-oid other) 24588
                       (gethash 24588 (pg::logical-subscription-runtime-relations runtime)) other)
                 (pg::logical-validate-stream-relation runtime other))
               (pg::logical-pgoutput-dispatch
                runtime (pg::pgoutput-build-begin
                         (pg::make-logical-transaction :xid 999 :commit-lsn 100)))
               (pg::logical-pgoutput-dispatch
                runtime (pg::pgoutput-build-truncate '(24587 24588)))
               (ok (null (pg::logical-target-query target "SELECT * FROM fixture_rows")))
               (ok (null (pg::logical-target-query target "SELECT * FROM fixture_other")))
               (ok (equalp '(#("8" "8"))
                           (pg::logical-target-query observer "SELECT * FROM fixture_rows")))
               (pg::logical-target-query target "ROLLBACK")
               (ok (equalp '(#("8" "8"))
                           (pg::logical-target-query observer "SELECT * FROM fixture_rows")))
               (ok (equalp '(#("9" "9"))
                           (pg::logical-target-query observer "SELECT * FROM fixture_other")))))
        (pg::pg-client-close target)
        (pg::pg-client-close observer))))
