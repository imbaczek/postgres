"""Defaults, CHECKs, and basic aggregate behavior against the owned C peer."""
import sys
from pathlib import Path
from subqueries import QueryConnection

CASES = (
    "CREATE SEQUENCE dc_seq",
    "CREATE TABLE dc_rows(id int PRIMARY KEY,a int DEFAULT 2+3 CHECK(a>0),b int DEFAULT nextval('dc_seq')+10,label varchar(12) DEFAULT lower('OK'),CONSTRAINT ordered CHECK(b>=a))",
    "INSERT INTO dc_rows(id) VALUES(1),(2) RETURNING *",
    "INSERT INTO dc_rows VALUES(3,NULL,DEFAULT,DEFAULT) RETURNING *",
    "UPDATE dc_rows SET a=DEFAULT,b=DEFAULT WHERE id=1 RETURNING *",
    "INSERT INTO dc_rows VALUES(4,-1,30,'bad')",
    "INSERT INTO dc_rows VALUES(1,-1,30,'bad') ON CONFLICT DO NOTHING",
    "INSERT INTO dc_rows VALUES(1,1,30,'x') ON CONFLICT(id) DO UPDATE SET a=-1",
    "UPDATE dc_rows SET b=1 WHERE id=1 RETURNING *",
    "SELECT * FROM dc_rows ORDER BY id",
    "ALTER TABLE dc_rows ALTER COLUMN a SET DEFAULT 7+1",
    "INSERT INTO dc_rows(id) VALUES(4) RETURNING *",
    "ALTER TABLE dc_rows ALTER a DROP DEFAULT",
    "INSERT INTO dc_rows(id) VALUES(5) RETURNING *",
    "ALTER TABLE dc_rows ADD CONSTRAINT old_bad CHECK(id<4)",
    "ALTER TABLE dc_rows ADD CONSTRAINT old_bad CHECK(id<4) NOT VALID",
    "INSERT INTO dc_rows(id) VALUES(6)",
    "UPDATE dc_rows SET label='other' WHERE id=4",
    "ALTER TABLE dc_rows VALIDATE CONSTRAINT old_bad",
    "DELETE FROM dc_rows WHERE id>=4",
    "ALTER TABLE dc_rows VALIDATE CONSTRAINT old_bad",
    "ALTER TABLE dc_rows VALIDATE CONSTRAINT old_bad",
    "ALTER TABLE dc_rows RENAME CONSTRAINT old_bad TO limited",
    "ALTER TABLE dc_rows DROP CONSTRAINT limited",
    "ALTER TABLE dc_rows VALIDATE CONSTRAINT dc_rows_pkey",
    "ALTER TABLE dc_rows ADD CONSTRAINT dc_rows_pkey CHECK(a>0)",
    "CREATE TABLE dc_other(a int,CONSTRAINT ordered CHECK(a>0))",
    "CREATE TABLE dc_cross(a int CHECK(a<b), b int)",
    "INSERT INTO dc_cross VALUES(1,2),(NULL,1)",
    "INSERT INTO dc_cross VALUES(2,1)",
    "CREATE TABLE dc_unknown(a int CHECK(NULL))",
    "INSERT INTO dc_unknown VALUES(1)",
    "CREATE TABLE dc_reject1(a int DEFAULT a+1)",
    "CREATE TABLE dc_reject2(a int DEFAULT (SELECT 1))",
    "CREATE TABLE dc_reject3(a int DEFAULT sum(1))",
    "CREATE TABLE dc_reject4(a int DEFAULT row_number() OVER())",
    "CREATE TABLE dc_reject5(a int CHECK(1))",
    "CREATE TABLE dc_reject6(a int CHECK((SELECT true)))",
    "CREATE TABLE dc_reject7(a int CHECK(sum(a)>0))",
    "CREATE TABLE dc_reject8(a int CHECK(row_number() OVER()>0))",
    "ALTER TABLE dc_rows ALTER a SET DEFAULT true",
    "ALTER TABLE dc_rows ALTER a SET DEFAULT 'bad'",
    "ALTER TABLE dc_rows ALTER label SET DEFAULT 'long default text'",
    "INSERT INTO dc_rows(id) VALUES(8)",
    "ALTER TABLE dc_rows ALTER label SET DEFAULT 'ok'",
    "DROP SEQUENCE dc_seq",
    "CREATE TABLE dc_notvalid(a int CHECK(a>0) NOT VALID)",
    "CREATE TABLE dc_ag(g int,x int,s varchar(12))",
    "INSERT INTO dc_ag VALUES(1,1,'b'),(1,2,'a'),(1,NULL,NULL),(2,0,'z'),(2,-1,'x')",
    "SELECT count(*),count(x),min(x),max(x),avg(x),min(s),max(s) FROM dc_ag",
    "SELECT g,count(x),min(x),max(x),avg(x) FROM dc_ag GROUP BY g ORDER BY g",
    "SELECT count(x),min(x),max(x),avg(x) FROM dc_ag WHERE false",
    "SELECT count(NULL),count(false),count(0),min(NULL),max(NULL)",
    "SELECT avg(NULL)",
    "SELECT min(true),max(false)",
    "SELECT avg(s) FROM dc_ag",
    "SELECT g FROM dc_ag GROUP BY g HAVING avg(x)>0 ORDER BY max(x)",
    "SELECT x,count(x) OVER(ORDER BY x),min(x) OVER(),max(s) OVER(),avg(x) OVER(ORDER BY x) FROM dc_ag ORDER BY x,s",
    "WITH q AS MATERIALIZED(SELECT g,avg(x) AS a FROM dc_ag GROUP BY g) SELECT g,a,a::integer,a+a,a*2,a/2 FROM q ORDER BY a",
    "WITH q AS(SELECT g,avg(x) AS a FROM dc_ag GROUP BY g) SELECT sum(a),avg(a),min(a),max(a),count(a) FROM q",
    "SELECT avg(x)::integer FROM (SELECT 0 AS x UNION ALL SELECT -1) d",
    "SELECT avg(x) FROM (SELECT 9223372036854775807::bigint AS x UNION ALL SELECT -9223372036854775808::bigint) d",
    "SELECT avg(x) FROM (SELECT 9999 AS x UNION ALL SELECT 10000) d",
    "SELECT avg(x) FROM (SELECT 1 AS x UNION ALL SELECT -1) d",
    "SELECT avg(x) FROM (SELECT 1 AS x UNION ALL SELECT 0 UNION ALL SELECT 0) d",
    "SELECT avg(x) FROM (SELECT -1 AS x UNION ALL SELECT 0 UNION ALL SELECT 0) d",
    "SELECT avg(x),sum(x),min(x),max(x) FROM (SELECT '1.00'::numeric AS x UNION ALL SELECT '2.0'::numeric) d",
    "SELECT DISTINCT x FROM (SELECT '1.0'::numeric AS x UNION ALL SELECT '1.00'::numeric) d",
    "SELECT x,count(*) FROM (SELECT '1.0'::numeric AS x UNION ALL SELECT '1.00'::numeric) d GROUP BY x",
    "SELECT '1.5'::numeric::integer,'-1.5'::numeric::integer,'0.5'::numeric::smallint",
    "SELECT '2147483647.5'::numeric::integer",
    "SELECT '-2147483648.5'::numeric::integer",
    "SELECT '9223372036854775807.5'::numeric::bigint",
    "SELECT avg(x)>'1' FROM (SELECT 1 AS x UNION ALL SELECT 2) d",
    "SELECT 1::numeric/3::numeric,1::numeric+2,1::numeric*2,1::numeric%2",
    "SELECT length((('1e131071'::numeric)*10)::text)",
    "SELECT length((('1e-10000'::numeric)*('1e-10000'::numeric))::text)",
    "CREATE TABLE dc_named(a int CONSTRAINT named_pk PRIMARY KEY, b int CONSTRAINT named_u UNIQUE, CONSTRAINT named_check CHECK(a>0))",
    "ALTER TABLE dc_named RENAME CONSTRAINT named_pk TO named_check",
    "ALTER TABLE dc_named RENAME CONSTRAINT named_check TO named_u",
    "ALTER TABLE dc_named ADD CONSTRAINT named_check UNIQUE(b)",
    "CREATE UNIQUE INDEX dc_standalone ON dc_named(b)",
    "ALTER TABLE dc_named ADD CONSTRAINT named_check UNIQUE USING INDEX dc_standalone",
    "ALTER TABLE dc_named ADD CONSTRAINT dc_standalone CHECK(b>0)",
    "CREATE TABLE dc_qualified(a int CHECK(other.a>0))",
    "CREATE TABLE dc_validqual(a int CHECK(dc_validqual.a>0))",
    "SELECT sum('1.0'::numeric),sum('1.00'::numeric),min('1.0'::numeric),min('1.00'::numeric)",
    "SELECT DISTINCT '1.0'::numeric ORDER BY '1.00'::numeric",
    "CREATE TABLE dc_decimal(a int DEFAULT '1.5'::numeric CHECK(a<'4.25'::numeric))",
    "INSERT INTO dc_decimal DEFAULT VALUES RETURNING *",
    "ALTER TABLE dc_decimal ADD CONSTRAINT decimal_later CHECK(a>'0.50'::numeric) NOT VALID",
    "INSERT INTO dc_decimal VALUES(5)",
    "CREATE SEQUENCE dc_order_seq",
    "CREATE TABLE dc_order(a int NOT NULL,CONSTRAINT z_last CHECK(nextval('dc_order_seq')>0),CONSTRAINT a_first CHECK(a>0))",
    "INSERT INTO dc_order VALUES(-1)",
    "SELECT currval('dc_order_seq')",
    "INSERT INTO dc_order VALUES(NULL)",
    "SELECT currval('dc_order_seq')",
    "INSERT INTO dc_order VALUES(1)",
    "SELECT currval('dc_order_seq')",
    "ALTER TABLE dc_order ADD CONSTRAINT volatile_validation CHECK(nextval('dc_order_seq')>0)",
    "SELECT currval('dc_order_seq')",
    "CREATE TABLE dc_default_notnull(a int DEFAULT 4 NOT NULL,b int DEFAULT 5 CHECK(b>0))",
    "INSERT INTO dc_default_notnull DEFAULT VALUES RETURNING *",
    "CREATE TABLE dc_bad_default_in(a int DEFAULT 1 IN (1,2))",
    "SELECT min('x'::name),max('x'::name),min('1'::oid),max('1'::oid)",
    "SELECT min(x),max(x) FROM (SELECT '1.0'::numeric AS x UNION ALL SELECT '1.00'::numeric) d",
    "SELECT '0e200001'::numeric",
    "SELECT '\v1\v'::numeric",
    "SELECT length(avg(x)::text) FROM (SELECT '9e131071'::numeric AS x UNION ALL SELECT '9e131071'::numeric) d",
    "SELECT length(sum(x)::text) FROM (SELECT '9e131071'::numeric AS x UNION ALL SELECT '9e131071'::numeric) d",
    "CREATE TABLE dc_fold(id int DEFAULT nextval('dc_seq'),v varchar(3) DEFAULT 'long')",
    "INSERT INTO dc_fold DEFAULT VALUES",
    "INSERT INTO dc_fold(id) SELECT 2 WHERE false",
    "UPDATE dc_fold SET v=DEFAULT WHERE false",
    "INSERT INTO dc_fold(v) VALUES(NULL) RETURNING *",
    "ALTER TABLE dc_fold ALTER v SET DEFAULT 'ok'",
    "CREATE TABLE dc_math(v int DEFAULT (nextval('dc_seq')+1/0))",
    "INSERT INTO dc_math DEFAULT VALUES",
    "INSERT INTO dc_math SELECT 1 WHERE false",
    "ALTER TABLE dc_math ALTER v SET DEFAULT CASE WHEN false THEN 1/0 ELSE 7 END",
    "INSERT INTO dc_math DEFAULT VALUES RETURNING *",
    "ALTER TABLE dc_math ALTER v SET DEFAULT CASE WHEN nextval('dc_seq')>0 THEN 7 ELSE 1/0 END",
    "INSERT INTO dc_math DEFAULT VALUES",
    "ALTER TABLE dc_math ALTER v SET DEFAULT COALESCE(7,1/0)",
    "INSERT INTO dc_math DEFAULT VALUES RETURNING *",
    "ALTER TABLE dc_math ALTER v SET DEFAULT CASE WHEN nextval('dc_seq')>0 AND false THEN 9 ELSE 7 END",
    "INSERT INTO dc_math DEFAULT VALUES RETURNING *",
    "SELECT currval('dc_seq')",
    "ALTER TABLE dc_math ALTER v SET DEFAULT length(substr(NULL,nextval('dc_seq')::int))",
    "INSERT INTO dc_math DEFAULT VALUES RETURNING *",
    "SELECT currval('dc_seq')",
    "INSERT INTO dc_rows(id) VALUES(1) ON CONFLICT(id) DO UPDATE SET label=DEFAULT WHERE false",
    "CREATE TABLE dc_plan_order(a int PRIMARY KEY DEFAULT 1/0,b varchar(1) DEFAULT 'long')",
    "INSERT INTO dc_plan_order(a) VALUES(1) ON CONFLICT(a) DO UPDATE SET a=DEFAULT",
    "INSERT INTO dc_plan_order(a,b) VALUES(1,DEFAULT),(2,DEFAULT) ON CONFLICT(a) DO UPDATE SET a=DEFAULT",
    "CREATE TABLE dc_strict(a int DEFAULT nextval('dc_seq'),b varchar(1) DEFAULT COALESCE(NULL::text || nextval('dc_seq')::text,'long'))",
    "INSERT INTO dc_strict DEFAULT VALUES",
    "ALTER TABLE dc_strict ALTER b SET DEFAULT (NULL::text || (1/0)::text)",
    "INSERT INTO dc_strict DEFAULT VALUES",
    "ALTER TABLE dc_strict ALTER b SET DEFAULT 'ok'",
    "CREATE TABLE dc_nullseq(a int DEFAULT setval('dc_seq',NULL::bigint),b int DEFAULT nextval(NULL))",
    "INSERT INTO dc_nullseq DEFAULT VALUES RETURNING *",
    "SELECT currval('dc_seq')",
    "CREATE TABLE dc_value_order(a int DEFAULT 1/0,b varchar(1) DEFAULT 'long')",
    "INSERT INTO dc_value_order(b,a) VALUES(DEFAULT,DEFAULT)",
    "INSERT INTO dc_value_order(b,a) VALUES(DEFAULT,DEFAULT),(DEFAULT,DEFAULT)",
    "INSERT INTO dc_value_order(b) VALUES(DEFAULT),(DEFAULT)",
    "INSERT INTO dc_value_order(a) VALUES(DEFAULT),(DEFAULT)",
    "CREATE TABLE dc_array_fold(a int DEFAULT nextval('dc_seq'),b varchar(1) DEFAULT COALESCE(array_length(NULL::text[],nextval('dc_seq')::int)::text,'long'))",
    "INSERT INTO dc_array_fold DEFAULT VALUES",
    "CREATE TABLE dc_admin_fold(a int DEFAULT (pg_cancel_backend(NULL::int))::int)",
    "INSERT INTO dc_admin_fold DEFAULT VALUES RETURNING *",
    "CREATE TABLE dc_checknull(a int CHECK(setval('dc_seq',NULL::bigint)>0))",
    "INSERT INTO dc_checknull VALUES(1)",
)


def check(reference, native):
    peers = [QueryConnection(port, "defaults-checks-aggregates") for port in (reference, native)]
    failures = []
    try:
        for peer in peers:
            for sql in CASES:
                if sql.startswith("CREATE TABLE "):
                    peer.observe("DROP TABLE IF EXISTS " + sql.split("(")[0].split()[2])
            for name in ("dc_seq", "dc_order_seq"):
                peer.observe("DROP SEQUENCE IF EXISTS " + name)
        for index, sql in enumerate(CASES, 1):
            outcomes = [peer.observe(sql) for peer in peers]
            if outcomes[0] != outcomes[1]:
                failures.append((index, sql, outcomes))
                print("defaults-checks-aggregates: FAIL", failures[-1], flush=True)
        assert not failures, failures
        print(f"defaults-checks-aggregates: {len(CASES)} C comparisons PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()


def check_restart(reference, native):
    peers = [QueryConnection(port, "default-check-restart") for port in (reference, native)]
    try:
        for sql in ("INSERT INTO dc_decimal DEFAULT VALUES RETURNING a",
                    "INSERT INTO dc_decimal VALUES(0)",
                    "INSERT INTO dc_decimal VALUES(5)",
                    "ALTER TABLE dc_decimal VALIDATE CONSTRAINT decimal_later",
                    "SELECT avg(a) FROM dc_decimal",
                    "INSERT INTO dc_rows(id) VALUES(9) RETURNING *"):
            outcomes = [peer.observe(sql) for peer in peers]
            assert outcomes[0] == outcomes[1], (sql, outcomes)
        print("defaults-checks: fresh-image defaults/decimal AST/CHECK/NOT VALID/sequence state PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()


if __name__ == "__main__":
    if "--restart" in sys.argv:
        check_restart(int(sys.argv[1]), int(sys.argv[2]))
    else:
        check(int(sys.argv[1]), int(sys.argv[2]))
        if len(sys.argv) >= 5:
            from default_check_concurrency import check as concurrency
            from basic_aggregate_resources import check as resources
            concurrency(int(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
            resources(int(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
