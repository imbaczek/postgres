"""Nonrecursive SELECT WITH scope, sharing and lazy producer comparisons."""
import sys
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from subqueries import QueryConnection
from subqueries_concurrency import control, spill_counter, assert_no_spill_files

SETUP = (
    "CREATE TABLE cte_input(id int,g int,label varchar(20))",
    "INSERT INTO cte_input VALUES(1,1,'a'),(2,1,'b'),(3,2,NULL)",
    "CREATE INDEX cte_input_id ON cte_input(id)",
    "CREATE SEQUENCE cte_seq",
    "CREATE PUBLICATION cte_pub FOR TABLE cte_input",
)

CASES = [
    "WITH q AS MATERIALIZED(SELECT (pg_get_publication_tables('cte_pub')).*) SELECT count(*) FROM q",
    "SELECT setval('cte_seq',1,false)",
    "WITH q AS NOT MATERIALIZED(SELECT d.x FROM (SELECT 1 AS x WINDOW w AS(ORDER BY nextval('cte_seq'))) d) SELECT a.x,b.x FROM q a CROSS JOIN q b",
    "SELECT currval('cte_seq')",
    "SELECT setval('cte_seq',1,false)",
    "WITH q AS NOT MATERIALIZED(SELECT 1 AS x WINDOW w AS(ORDER BY nextval('cte_seq') IN(SELECT 1))) SELECT a.x,b.x FROM q a CROSS JOIN q b",
    "SELECT currval('cte_seq')",
    "WITH q AS(SELECT 7 AS x) SELECT x FROM q",
    "SELECT setval('cte_seq',1,false)",
    "WITH q AS MATERIALIZED(SELECT nextval('cte_seq') AS n WINDOW w AS(ORDER BY nextval('cte_seq'))) SELECT n FROM q",
    "SELECT currval('cte_seq')",
    "SELECT setval('cte_seq',1,false)",
    "WITH q AS MATERIALIZED(SELECT nextval('cte_seq')+1 AS n WINDOW w AS(ORDER BY nextval('cte_seq'))) SELECT n FROM q",
    "SELECT currval('cte_seq')",
    "WITH q AS NOT MATERIALIZED(SELECT d.x FROM (SELECT 1 AS x WINDOW w AS(ORDER BY 1/0)) d) SELECT * FROM q",
    "WITH q AS MATERIALIZED(SELECT g,sum(id) AS n FROM cte_input GROUP BY g WINDOW w AS(ORDER BY sum(id))) SELECT * FROM q ORDER BY g",
    "SELECT setval('cte_seq',1,false)",
    "WITH q AS MATERIALIZED(SELECT nextval('cte_seq')::int4+100 AS a,row_number() OVER w2 AS rn WINDOW w1 AS(ORDER BY nextval('cte_seq')::int4+100),w2 AS(ORDER BY nextval('cte_seq')::int4+200)) SELECT * FROM q",
    "SELECT currval('cte_seq')",
    "WITH q(x) AS(SELECT 7) SELECT a.x,b.x FROM q a CROSS JOIN q b",
    "WITH q(x) AS(SELECT 7,8 AS y) SELECT * FROM q",
    "WITH q(x,y) AS(SELECT 7) SELECT * FROM q",
    "WITH q(x,x) AS(SELECT 7,8) SELECT x FROM q",
    "WITH q AS(SELECT 7 AS x),r AS(SELECT x+1 AS y FROM q) SELECT y FROM r",
    "WITH q AS(SELECT 7 AS x) SELECT (WITH q AS(SELECT 8 AS x) SELECT x FROM q),x FROM q",
    "WITH a AS(SELECT 1 AS x),b AS(SELECT x FROM a) SELECT (WITH a AS(SELECT 2 AS x) SELECT x FROM b)",
    "WITH q AS(SELECT x FROM later),later AS(SELECT 1 AS x) SELECT * FROM q",
    "WITH q AS(SELECT * FROM q) SELECT * FROM q",
    "WITH q AS(SELECT 1),q AS(SELECT 2) SELECT 3",
    "WITH q AS(SELECT nonexistent) SELECT 3",
    "WITH q AS(SELECT missing()) SELECT 3",
    "WITH q AS(SELECT 1/0) SELECT 3",
    "WITH q AS(SELECT row_number() OVER(ORDER BY 1/0)) SELECT 3",
    "WITH cte_input AS(SELECT 8 AS id) SELECT id FROM cte_input",
    "WITH cte_input AS(SELECT 8 AS id) SELECT id FROM public.cte_input ORDER BY id",
    "WITH q AS(SELECT * FROM cte_input) SELECT * FROM q ORDER BY id",
    "WITH q AS MATERIALIZED(SELECT * FROM cte_input) SELECT * FROM q ORDER BY id",
    "WITH q AS NOT MATERIALIZED(SELECT * FROM cte_input) SELECT a.id,b.id FROM q a JOIN q b ON a.id=b.id ORDER BY a.id",
    "WITH q AS(SELECT id FROM cte_input) SELECT id FROM q UNION ALL SELECT id+10 FROM q ORDER BY id",
    "SELECT (WITH q AS(SELECT 9 AS x) SELECT x FROM q)",
    "SELECT 9 IN(WITH q AS(SELECT 9 AS x) SELECT x FROM q)",
    "WITH q AS MATERIALIZED(SELECT 10/(2-id) AS v FROM cte_input) SELECT v FROM q LIMIT 1",
    "WITH q AS MATERIALIZED(SELECT 10/(2-id) AS v FROM cte_input) SELECT EXISTS(SELECT 1 FROM q)",
    "SELECT setval('cte_seq',1,false)",
    "WITH q AS(SELECT nextval('cte_seq') AS n FROM cte_input) SELECT 1",
    "SELECT nextval('cte_seq')",
    "SELECT setval('cte_seq',1,false)",
    "WITH q AS(SELECT nextval('cte_seq') AS n FROM cte_input) SELECT a.n,b.n FROM q a CROSS JOIN q b LIMIT 1",
    "SELECT currval('cte_seq')",
    "SELECT setval('cte_seq',1,false)",
    "WITH q AS NOT MATERIALIZED(SELECT nextval('cte_seq') AS n FROM cte_input) SELECT a.n,b.n FROM q a CROSS JOIN q b ORDER BY a.n,b.n",
    "SELECT currval('cte_seq')",
    "SELECT setval('cte_seq',1,false)",
    "WITH q AS MATERIALIZED(SELECT nextval('cte_seq') AS n FROM cte_input) SELECT (SELECT n FROM q LIMIT 1),n FROM q ORDER BY n",
    "SELECT currval('cte_seq')",
    "SELECT setval('cte_seq',1,false)",
    "SELECT o.id,(WITH q AS MATERIALIZED(SELECT nextval('cte_seq') AS n) SELECT n+o.id*0 FROM q) FROM cte_input o ORDER BY o.id",
    "SELECT currval('cte_seq')",
    "SELECT setval('cte_seq',1,false)",
    "SELECT o.id,(WITH q AS MATERIALIZED(SELECT nextval('cte_seq')+o.id*0 AS n) SELECT a.n+b.n FROM q a CROSS JOIN q b) FROM cte_input o ORDER BY o.id",
    "SELECT currval('cte_seq')",
    "WITH q AS(SELECT label FROM cte_input) SELECT array_agg(label::text) FROM q",
    "WITH q AS MATERIALIZED(SELECT id,g FROM cte_input) SELECT g,sum(id),count(*) FROM q GROUP BY g ORDER BY g",
    "WITH q AS MATERIALIZED(SELECT DISTINCT g FROM cte_input) SELECT g FROM q ORDER BY g",
    "WITH q AS MATERIALIZED(SELECT id,sum(id) OVER(ORDER BY id) AS s,rank() OVER(ORDER BY g) AS r FROM cte_input) SELECT * FROM q ORDER BY id",
    "SELECT setval('cte_seq',1,false)",
    "WITH q AS MATERIALIZED(SELECT sum(nextval('cte_seq')) OVER(ROWS UNBOUNDED PRECEDING) AS s FROM cte_input) SELECT s FROM q LIMIT 1",
    "SELECT currval('cte_seq')",
    "SELECT setval('cte_seq',1,false)",
    "WITH q AS MATERIALIZED(SELECT sum(nextval('cte_seq')) OVER(ORDER BY g) AS s FROM cte_input) SELECT s FROM q LIMIT 1",
    "SELECT currval('cte_seq')",
    "WITH q AS MATERIALIZED(SELECT a.id AS x,b.id AS y FROM cte_input a FULL JOIN cte_input b ON a.id+1=b.id) SELECT * FROM q ORDER BY x,y",
    "SELECT g,(WITH q AS MATERIALIZED(SELECT o.g AS v) SELECT sum(v) FROM q) FROM cte_input o GROUP BY g ORDER BY g",
    "SELECT g,(WITH q AS MATERIALIZED(SELECT o.id AS v) SELECT sum(v) FROM q) FROM cte_input o GROUP BY g ORDER BY g",
    "WITH q AS(SELECT 1 AS x) SELECT EXISTS(WITH r AS(SELECT 2 AS x) SELECT x FROM r WHERE false)",
    "WITH q AS(SELECT 1 WINDOW w AS(ORDER BY 1/0)) SELECT 3",
    "WITH q AS(SELECT 1 WINDOW w AS(ORDER BY 1/0)) SELECT * FROM q",
    "WITH q AS MATERIALIZED(SELECT 1 WINDOW w AS(ORDER BY 1/0)) SELECT * FROM q",
    "WITH q AS NOT MATERIALIZED(SELECT 1 WINDOW w AS(ORDER BY 1/0)) SELECT * FROM q",
    "WITH q AS(SELECT 1 AS x WINDOW w AS(ORDER BY 1/0)) SELECT a.x,b.x FROM q a CROSS JOIN q b",
    "WITH q AS NOT MATERIALIZED(SELECT id,10/(2-id) AS v FROM cte_input) SELECT v FROM q WHERE id=1",
    "WITH q AS(SELECT id,10/(2-id) AS v FROM cte_input) SELECT v FROM q WHERE id=1",
    "WITH q AS MATERIALIZED(SELECT id,10/(2-id) AS v FROM cte_input) SELECT v FROM q WHERE id=1",
    "WITH q AS(SELECT id,current_setting(CASE WHEN id=1 THEN 'application_name' ELSE 'cte_missing_setting' END) AS v FROM cte_input) SELECT v FROM q WHERE id=1",
    "WITH q AS NOT MATERIALIZED(SELECT id,current_setting(CASE WHEN id=1 THEN 'application_name' ELSE 'cte_missing_setting' END) AS v FROM cte_input) SELECT v FROM q WHERE id=1",
    "WITH q AS MATERIALIZED(SELECT id,current_setting(CASE WHEN id=1 THEN 'application_name' ELSE 'cte_missing_setting' END) AS v FROM cte_input) SELECT v FROM q WHERE id=1",
    "SELECT setval('cte_seq',1,false)",
    "WITH q AS MATERIALIZED(SELECT nextval('cte_seq') AS n FROM cte_input) SELECT * FROM q WHERE false",
    "SELECT nextval('cte_seq')",
    "CREATE TABLE cte_target(id int)",
    "INSERT INTO cte_target VALUES((WITH q AS MATERIALIZED(SELECT 7 AS x UNION ALL SELECT 8) SELECT x FROM q LIMIT 1))",
    "UPDATE cte_target SET id=(WITH q AS MATERIALIZED(SELECT cte_target.id+1 AS x) SELECT x FROM q)",
    "SELECT id FROM cte_target",
    "WITH q AS NOT MATERIALIZED(SELECT id FROM cte_input) SELECT a.x FROM (SELECT 1 AS x) a LEFT JOIN q ON q.id=a.x WHERE q.id IS NULL",
    "WITH q AS(SELECT id FROM cte_input) SELECT a.x FROM (SELECT 1 AS x) a LEFT JOIN q ON q.id=a.x WHERE coalesce(q.id,0)=0",
    "WITH q AS(SELECT id FROM cte_input) SELECT a.x FROM q RIGHT JOIN (SELECT 1 AS x) a ON q.id=a.x WHERE q.id IS NULL",
]

for policy in ("", "MATERIALIZED", "NOT MATERIALIZED"):
    for body in (
        "SELECT id FROM cte_input WHERE id>1 ORDER BY id DESC LIMIT 1",
        "SELECT id,label FROM cte_input ORDER BY id",
        "SELECT count(*) AS n FROM cte_input WHERE false",
        "SELECT g,array_agg(label::text) AS labels FROM cte_input GROUP BY g ORDER BY g",
        "SELECT a.id AS x,b.id AS y FROM cte_input a LEFT JOIN cte_input b ON a.id+1=b.id ORDER BY a.id",
        "SELECT id,(SELECT label FROM cte_input i WHERE i.id=o.id) AS label FROM cte_input o ORDER BY id",
    ):
        CASES.append(f"WITH q AS {policy}({body}) SELECT * FROM q")


def check(reference, native, base, socket):
    peers = [QueryConnection(reference, "ctes"), QueryConnection(native, "ctes")]
    try:
        for sql in SETUP:
            results = [peer.observe(sql) for peer in peers]
            assert results[0] == results[1], (sql, results)
        for index, sql in enumerate(CASES, 1):
            results = [peer.observe(sql) for peer in peers]
            assert results[0] == results[1], (index, sql, results)
            print(f"ctes: {index}: PASS", flush=True)
        print(f"ctes: {len(CASES)} C differential cases PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()
    results = [resources(port, native_p, base, socket)
               for port, native_p in ((reference, False), (native, True))]
    assert results[0] == results[1], results
    print("ctes: spill, late error, cancellation and same-connection recovery PASS", flush=True)


def resources(port, native, base, socket):
    connection, admin = [QueryConnection(port, "cte-resources") for _ in range(2)]
    pool = ThreadPoolExecutor(max_workers=1)
    old_memory = None
    future = None
    try:
        admin.query("CREATE TABLE cte_big(id int,label text)")
        admin.query("INSERT INTO cte_big VALUES " + ",".join(
            f"({i},'{str(i).zfill(3)}{'x'*93}')" for i in range(300)))
        if native:
            output = control(base, socket, '(format t "CTE_MEMORY=~d~%" pg::*query-work-memory-bytes*)')
            old_memory = int(re.search(r"CTE_MEMORY=(\d+)", output)[1])
            control(base, socket, '(setf pg::*query-work-memory-bytes* 8192)')
            before = spill_counter(base, socket)
        full = connection.observe(
            "WITH q AS MATERIALIZED(SELECT id,label FROM cte_big) SELECT count(*) FROM q a CROSS JOIN q b")
        assert full[0] == "rows", full
        windows = connection.observe(
            "WITH q AS MATERIALIZED(SELECT id,array_agg(id::text) OVER(ORDER BY id) AS a FROM cte_big) SELECT * FROM q ORDER BY id LIMIT 3")
        assert windows[0] == "rows", windows
        late = connection.observe(
            "WITH q AS MATERIALIZED(SELECT label,10/(299-id) AS n FROM cte_big) SELECT sum(n) FROM q")
        assert late == ("error", "22012"), late
        inserted = connection.observe(
            "INSERT INTO cte_target VALUES((WITH q AS MATERIALIZED(SELECT id,label FROM cte_big) SELECT sum(id)::int4 FROM q))")
        updated = connection.observe(
            "UPDATE cte_target SET id=(WITH q AS MATERIALIZED(SELECT cte_target.id AS id,cte_big.label FROM cte_big) SELECT sum(id)::int4 FROM q)")
        assert inserted[0] == updated[0] == "rows", (inserted, updated)
        target_values = connection.observe("SELECT id FROM cte_target ORDER BY id")
        if native:
            assert spill_counter(base, socket) > before, "CTE queries did not spill"
            assert_no_spill_files(base, socket)
            before = spill_counter(base, socket)
        future = pool.submit(connection.observe,
                             "WITH q AS MATERIALIZED(SELECT a.id FROM cte_big a CROSS JOIN cte_big b CROSS JOIN cte_big c) SELECT count(*) FROM q")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert not future.done(), future.result()
            ready = (spill_counter(base, socket) > before if native else
                     admin.query(f"SELECT state FROM pg_stat_activity WHERE pid={connection.backend_pid}") == [["active"]])
            if ready:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("CTE producer did not start")
        assert admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})") == [["t"]]
        cancelled = future.result(timeout=20)
        assert cancelled == ("error", "57014"), cancelled
        if native:
            assert_no_spill_files(base, socket)
            before = spill_counter(base, socket)
        future = pool.submit(
            connection.observe,
            "INSERT INTO cte_target VALUES((WITH q AS MATERIALIZED(SELECT a.id FROM cte_big a CROSS JOIN cte_big b CROSS JOIN cte_big c) SELECT count(*)::int4 FROM q))")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert not future.done(), future.result()
            ready = (spill_counter(base, socket) > before if native else
                     admin.query(f"SELECT state FROM pg_stat_activity WHERE pid={connection.backend_pid}") == [["active"]])
            if ready:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("DML CTE producer did not start")
        assert admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})") == [["t"]]
        cancelled_dml = future.result(timeout=20)
        assert cancelled_dml == ("error", "57014"), cancelled_dml
        assert connection.observe("SELECT id FROM cte_target ORDER BY id") == target_values
        after = connection.observe("WITH q AS(SELECT 7 AS x) SELECT x FROM q")
        if native:
            assert_no_spill_files(base, socket)
        return full, windows, late, inserted, updated, target_values, cancelled, cancelled_dml, after
    finally:
        if future is not None and not future.done():
            admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})")
        pool.shutdown(wait=True, cancel_futures=True)
        if old_memory is not None:
            control(base, socket, f'(setf pg::*query-work-memory-bytes* {old_memory})')
        connection.close()
        admin.close()


if __name__ == "__main__":
    check(int(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
