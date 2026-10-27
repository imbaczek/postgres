"""DML result metadata, statement snapshots, coercions and conflict semantics."""
import sys
from pathlib import Path
from subqueries import QueryConnection
from dml_resources import check as check_resources
from dml_concurrency import check as check_concurrency

SETUP = (
    "CREATE SEQUENCE dml_seq",
    "CREATE TABLE dml_source(id int,label varchar(5),v int)",
    "INSERT INTO dml_source VALUES(1,'one',10),(2,'two',20),(3,NULL,30)",
    "CREATE INDEX dml_source_id ON dml_source(id)",
    "CREATE TABLE dml_target(id int PRIMARY KEY,label varchar(5) UNIQUE,v int DEFAULT nextval('dml_seq'))",
    "CREATE TABLE dml_nullable(id int UNIQUE NULLS NOT DISTINCT,v int)",
    "CREATE TABLE dml_self(id int PRIMARY KEY,v int)",
    "INSERT INTO dml_self VALUES(1,10),(2,20)",
)

CASES = [
    "INSERT INTO dml_target SELECT * FROM dml_source ORDER BY id RETURNING *",
    "INSERT INTO dml_target SELECT 9,'nine',90 WHERE false RETURNING id,label",
    "INSERT INTO dml_target(id) SELECT 4 RETURNING id,label,v",
    "INSERT INTO dml_target SELECT 5 RETURNING id,label,v",
    "INSERT INTO dml_target(id,label,v) SELECT '6','six','60' RETURNING id,v",
    "INSERT INTO dml_target(id,label) WITH q AS(SELECT 7 AS id,'seven' AS label) SELECT * FROM q RETURNING *",
    "INSERT INTO dml_target AS d(id,label) VALUES(8,'eight') RETURNING d.*,old.id,new.id,id+1 AS next_id",
    "INSERT INTO dml_target(id) VALUES(9) RETURNING WITH(OLD AS o,NEW AS n) o.id,n.id",
    "INSERT INTO dml_target(id) SELECT 10 RETURNING id,(SELECT count(*) FROM dml_target) AS before_count",
    "INSERT INTO dml_target(id) VALUES(11),(12) RETURNING id,(SELECT count(*) FROM dml_target) AS before_count",
    "INSERT INTO dml_target(id) VALUES(13) RETURNING (SELECT dml_target.id+1)",
    "INSERT INTO dml_target(id) SELECT 14 RETURNING missing",
    "SELECT id FROM dml_target WHERE id=14",
    "INSERT INTO dml_target(id,label) SELECT 14,'too long' RETURNING *",
    "SELECT id FROM dml_target WHERE id=14",
    "INSERT INTO dml_target(id,label) SELECT 14,'four   ' RETURNING label",
    "INSERT INTO dml_target(id,label) SELECT 15,'fifth'::varchar RETURNING id,label",
    "INSERT INTO dml_target(id,label) SELECT 16,'bad'::text RETURNING count(*)",
    "INSERT INTO dml_target(id,label) SELECT 16,'bad'::text RETURNING row_number() OVER()",
    "INSERT INTO dml_target(id,label) SELECT 16,'bad'::text RETURNING pg_get_publication_tables('x')",
    "INSERT INTO dml_target(id) SELECT 16,17",
    "INSERT INTO dml_target(id,label) SELECT 16",
    "INSERT INTO dml_target(id,id) SELECT 16,17",
    "INSERT INTO dml_target(id) SELECT 'bad'",
    "INSERT INTO dml_target(id) SELECT '17'::text",
    "INSERT INTO dml_target(id) SELECT 2147483648::bigint",
    "INSERT INTO dml_self SELECT id+10,v FROM dml_self ORDER BY id RETURNING *",
    "SELECT id,v FROM dml_self ORDER BY id",
    "INSERT INTO dml_target(id) VALUES(20),(21) RETURNING 1/(21-id)",
    "SELECT id FROM dml_target WHERE id>=20 ORDER BY id",
    "UPDATE dml_target AS d SET v=v+1 WHERE id=1 RETURNING d.*,old.v,new.v",
    "UPDATE dml_target SET v=0 WHERE false RETURNING *",
    "DELETE FROM dml_target AS d WHERE id=2 RETURNING d.*,old.id,new.id",
    "DELETE FROM dml_target WHERE false RETURNING id,label",
    "DELETE FROM dml_target WHERE id=3 RETURNING WITH(OLD AS o,NEW AS n) o.*,n.id",
    "DELETE FROM dml_target WHERE id=4 RETURNING (SELECT dml_target.id),id,(SELECT count(*) FROM dml_target)",
    "DELETE FROM dml_target WHERE id=5 RETURNING 1/0",
    "SELECT id FROM dml_target WHERE id=5",
    "INSERT INTO dml_target(id,label,v) VALUES(1,'other',99) ON CONFLICT DO NOTHING RETURNING *",
    "INSERT INTO dml_target(id,label,v) VALUES(30,'other',99) ON CONFLICT DO NOTHING RETURNING *",
    "INSERT INTO dml_target(id,label,v) VALUES(1,'changed',99) ON CONFLICT(id) DO NOTHING RETURNING *",
    "INSERT INTO dml_target(id,label,v) VALUES(1,'other',99) ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING id,v,old.v,new.v",
    "INSERT INTO dml_target AS d(id,label,v) SELECT 1,'x',100 ON CONFLICT(id) DO UPDATE SET v=d.v+excluded.v WHERE excluded.v>0 RETURNING d.*,old.v,new.v",
    "INSERT INTO dml_target(id,v) VALUES(1,0) ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE false RETURNING id,v",
    "INSERT INTO dml_target(id,v) VALUES(1,101) ON CONFLICT ON CONSTRAINT dml_target_pkey DO UPDATE SET v=excluded.v RETURNING id,v",
    "INSERT INTO dml_target(id,v) VALUES(1,102) ON CONFLICT(label) DO NOTHING RETURNING id",
    "INSERT INTO dml_target(id,v) VALUES(1,102) ON CONFLICT(v) DO NOTHING RETURNING id",
    "INSERT INTO dml_target(id,v) VALUES(1,102) ON CONFLICT DO UPDATE SET v=excluded.v",
    "INSERT INTO dml_target(id,v) VALUES(40,1),(40,2) ON CONFLICT(id) DO NOTHING RETURNING id,v",
    "INSERT INTO dml_target(id,v) VALUES(41,1),(41,2) ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING id,v",
    "SELECT id,v FROM dml_target WHERE id=41",
    "INSERT INTO dml_target(id,v) VALUES(1,1),(1,2) ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING id,v",
    "SELECT id,v FROM dml_target WHERE id=1",
    "INSERT INTO dml_target(id,v) VALUES(1,0),(1,200) ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE excluded.v>0 RETURNING id,v",
    "INSERT INTO dml_target(id,v) VALUES(1,201) ON CONFLICT(id) DO UPDATE SET v=(SELECT excluded.v+1) RETURNING id,v",
    "INSERT INTO dml_target(id,v) VALUES(1,202) ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING excluded.v",
    "INSERT INTO dml_nullable VALUES(NULL,1) ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING *",
    "INSERT INTO dml_nullable VALUES(NULL,2) ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING old.v,new.v",
    "INSERT INTO dml_target(id,v) SELECT id,1000 FROM dml_source WHERE true ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING id,v",
    "SELECT id,v FROM dml_target ORDER BY id",
    "BEGIN",
    "INSERT INTO dml_target(id) VALUES(50),(51)",
    "UPDATE dml_target SET v=9 WHERE id=50 RETURNING id,(SELECT count(*) FROM dml_target)",
    "DELETE FROM dml_target WHERE id=51 RETURNING id,(SELECT count(*) FROM dml_target)",
    "SELECT id FROM dml_target WHERE id>=50 ORDER BY id",
    "ROLLBACK",
    "WITH q AS(SELECT 55 AS id) INSERT INTO dml_target(id) SELECT id FROM q RETURNING id,(SELECT id FROM q)",
    "WITH q AS(SELECT 55 AS id) UPDATE dml_target SET v=99 WHERE id=(SELECT id FROM q) RETURNING id,v",
    "WITH q AS(SELECT 55 AS id) DELETE FROM dml_target WHERE id=(SELECT id FROM q) RETURNING id",
    "INSERT INTO dml_target(id) VALUES(56) ON CONFLICT(id) DO UPDATE SET v=v+1",
    "UPDATE dml_target SET v=1,v=2 RETURNING id",
    "CREATE TABLE dml_defaults(a int,b int)",
    "INSERT INTO dml_defaults VALUES(1)",
    "SELECT * FROM dml_defaults",
    "BEGIN",
    "INSERT INTO dml_target(id,v) VALUES(70,1),(71,2)",
    "UPDATE dml_target SET v=123 WHERE id=70 RETURNING id,old.v,new.v,(SELECT count(*) FROM dml_target)",
    "DELETE FROM dml_target WHERE id=71 RETURNING id,(SELECT count(*) FROM dml_target)",
    "COMMIT",
    "SELECT id,v FROM dml_target WHERE id>=70 ORDER BY id",
    "SELECT setval('dml_seq',1,false)",
    "WITH q AS MATERIALIZED(SELECT nextval('dml_seq') AS n) INSERT INTO dml_target(id) SELECT n+100 FROM q RETURNING id,(SELECT n FROM q)",
    "SELECT currval('dml_seq')",
    "WITH q AS(SELECT 1/0) INSERT INTO dml_target(id) VALUES(102) RETURNING id",
    "WITH RECURSIVE q(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM q WHERE n<3) INSERT INTO dml_target(id) SELECT n+110 FROM q RETURNING id",
    "CREATE TABLE dml_order(a int DEFAULT nextval('dml_seq'),b int)",
    "SELECT setval('dml_seq',1,false)",
    "INSERT INTO dml_order(b,a) VALUES(nextval('dml_seq'),nextval('dml_seq')) RETURNING *",
    "SELECT setval('dml_seq',1,false)",
    "INSERT INTO dml_order(b) VALUES(nextval('dml_seq')) RETURNING *",
    "SELECT setval('dml_seq',1,false)",
    "UPDATE dml_order SET b=nextval('dml_seq'),a=nextval('dml_seq') RETURNING *",
    "SELECT setval('dml_seq',1,false)",
    "INSERT INTO dml_order(b,a) VALUES(nextval('dml_seq'),nextval('dml_seq')),(nextval('dml_seq'),nextval('dml_seq')) RETURNING *",
    "SELECT setval('dml_seq',1,false)",
    "INSERT INTO dml_order(b) VALUES(nextval('dml_seq')),(nextval('dml_seq')) RETURNING *",
    "SELECT setval('dml_seq',1,false)",
    "INSERT INTO dml_order(b,a) SELECT nextval('dml_seq'),nextval('dml_seq') RETURNING *",
    "SELECT setval('dml_seq',1,false)",
    "INSERT INTO dml_order(b) SELECT nextval('dml_seq') RETURNING *",
    "SELECT setval('dml_seq',1,false)",
    "INSERT INTO dml_order(b,a) VALUES(nextval('dml_seq'),DEFAULT),(nextval('dml_seq'),DEFAULT) RETURNING *",
]


def check(reference, native, base, socket):
    peers = [QueryConnection(port, "dml") for port in (reference, native)]
    try:
        for sql in SETUP:
            outcomes = [peer.observe(sql) for peer in peers]
            assert outcomes[0] == outcomes[1], (sql, outcomes)
        failures = []
        for index, sql in enumerate(CASES, 1):
            outcomes = [peer.observe(sql) for peer in peers]
            if outcomes[0] != outcomes[1]:
                failures.append((index, sql, outcomes))
                print(f"dml: FAIL {failures[-1]!r}", flush=True)
            else:
                print(f"dml: {index}: PASS", flush=True)
        assert not failures, failures
        print(f"dml: {len(CASES)} C differential cases PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()
    check_concurrency(reference, native, base, socket)
    check_resources(reference, native, base, socket)


def check_restart(reference, native):
    peers = [QueryConnection(port, "dml-restart") for port in (reference, native)]
    try:
        for sql in ("SELECT id,label,v FROM dml_target ORDER BY id",
                    "SELECT * FROM dml_nullable", "SELECT * FROM dml_self ORDER BY id",
                    "SELECT count(*) FROM dml_sink", "SELECT count(*) FROM dml_cancel",
                    "INSERT INTO dml_target(id) VALUES(70) ON CONFLICT(id) DO UPDATE SET v=excluded.id RETURNING id,old.v,new.v",
                    "DELETE FROM dml_target WHERE id=70 RETURNING id,v"):
            outcomes = [peer.observe(sql) for peer in peers]
            assert outcomes[0] == outcomes[1], (sql, outcomes)
    finally:
        for peer in peers:
            peer.close()
    print("dml: committed heap, indexes, combo CIDs and conflict updates survive restart PASS", flush=True)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[3] == "restart":
        check_restart(int(sys.argv[1]), int(sys.argv[2]))
    else:
        check(int(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
