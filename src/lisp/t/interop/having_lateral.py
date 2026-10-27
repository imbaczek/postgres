"""HAVING, LATERAL and their CTE/window interactions against repository C."""
import sys
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from subqueries import QueryConnection
from subqueries_concurrency import control, spill_counter, assert_no_spill_files

SETUP = (
    "CREATE TABLE hl_input(id int,g int,v int)",
    "INSERT INTO hl_input VALUES(1,1,10),(2,1,20),(3,2,NULL),(4,3,40)",
    "CREATE INDEX hl_input_id ON hl_input(id)",
    "CREATE SEQUENCE hl_seq",
    "CREATE PUBLICATION hl_pub FOR TABLE hl_input",
)

CASES = [
    "SELECT g,sum(v) AS total FROM hl_input GROUP BY g HAVING sum(v)>15 ORDER BY g",
    "SELECT g FROM hl_input GROUP BY g HAVING count(*)>1 ORDER BY g",
    "SELECT sum(v) FROM hl_input HAVING count(*)=4",
    "SELECT sum(v) FROM hl_input HAVING count(*)=0",
    "SELECT count(*) FROM hl_input WHERE false HAVING count(*)=0",
    "SELECT sum(v) FROM hl_input WHERE false HAVING sum(v) IS NULL",
    "SELECT 1 FROM hl_input HAVING true",
    "SELECT 1 FROM hl_input WHERE false HAVING true",
    "SELECT 1 HAVING true",
    "SELECT 1 HAVING false",
    "SELECT 1 HAVING NULL",
    "SELECT id FROM hl_input HAVING true",
    "SELECT g FROM hl_input GROUP BY g HAVING id>0",
    "SELECT g AS z FROM hl_input GROUP BY g HAVING z>0",
    "SELECT 1 HAVING 1",
    "SELECT 1 HAVING row_number() OVER()>0",
    "SELECT 1 HAVING sum(count(*))>0",
    "SELECT g,count(*) FROM hl_input GROUP BY g HAVING g>1 ORDER BY g",
    "SELECT g,count(*) FROM hl_input GROUP BY g HAVING g>1 AND count(*)=1 ORDER BY g",
    "SELECT g,count(*) FROM hl_input GROUP BY g HAVING g>1 OR count(*)>1 ORDER BY g",
    "SELECT g,count(*) FROM hl_input GROUP BY g HAVING EXISTS(SELECT 1 WHERE g=1) ORDER BY g",
    "SELECT g,count(*) FROM hl_input GROUP BY g HAVING EXISTS(SELECT 1 WHERE id=1) ORDER BY g",
    "SELECT EXISTS(SELECT 1 FROM hl_input HAVING false)",
    "SELECT EXISTS(SELECT 1 FROM hl_input WHERE false HAVING true)",
    "SELECT 1 IN(SELECT count(*) FROM hl_input HAVING count(*)>0)",
    "SELECT g,sum(v),row_number() OVER(ORDER BY g) FROM hl_input GROUP BY g HAVING sum(v)>15 ORDER BY g",
    "WITH q AS MATERIALIZED(SELECT g,sum(v) AS total FROM hl_input GROUP BY g HAVING sum(v)>15) SELECT * FROM q ORDER BY g",
    "SELECT setval('hl_seq',1,false)",
    "SELECT sum(nextval('hl_seq')) FROM hl_input HAVING false",
    "SELECT nextval('hl_seq')",
    "SELECT setval('hl_seq',1,false)",
    "SELECT 1 FROM hl_input WHERE nextval('hl_seq')>0 HAVING true",
    "SELECT nextval('hl_seq')",
    "SELECT o.id,d.x FROM hl_input o CROSS JOIN LATERAL(SELECT o.id+1 AS x) d ORDER BY o.id",
    "SELECT o.id,d.x FROM hl_input o,LATERAL(SELECT o.id+1 AS x) d ORDER BY o.id",
    "SELECT o.id,d.x FROM hl_input o JOIN LATERAL(SELECT o.id+1 AS x) d ON d.x>2 ORDER BY o.id",
    "SELECT o.id,d.x FROM hl_input o LEFT JOIN LATERAL(SELECT o.id+1 AS x WHERE o.id<3) d ON true ORDER BY o.id",
    "SELECT o.id,d.x FROM hl_input o LEFT JOIN LATERAL(SELECT o.id+1 AS x) d ON false ORDER BY o.id",
    "SELECT o.id,d.x FROM hl_input o CROSS JOIN LATERAL(SELECT i.id AS x FROM hl_input i WHERE i.id<=o.id ORDER BY i.id DESC LIMIT 1) d ORDER BY o.id",
    "SELECT o.id,d.n FROM hl_input o CROSS JOIN LATERAL(SELECT count(*) AS n FROM hl_input i WHERE i.id<o.id) d ORDER BY o.id",
    "SELECT o.id,d.n FROM hl_input o LEFT JOIN LATERAL(SELECT count(*) AS n FROM hl_input i WHERE i.id<o.id HAVING count(*)>0) d ON true ORDER BY o.id",
    "SELECT o.id,d.n FROM hl_input o CROSS JOIN LATERAL(SELECT count(*) AS n FROM hl_input i HAVING o.id>2) d ORDER BY o.id",
    "SELECT o.id,d.x FROM hl_input o CROSS JOIN LATERAL(SELECT (SELECT o.id) AS x) d ORDER BY o.id",
    "SELECT o.id,d.x FROM hl_input o CROSS JOIN LATERAL(SELECT s.x FROM LATERAL(SELECT o.id AS x) s) d ORDER BY o.id",
    "SELECT o.id,d.x FROM hl_input o CROSS JOIN LATERAL(WITH q AS MATERIALIZED(SELECT o.id AS x) SELECT x FROM q) d ORDER BY o.id",
    "WITH q AS MATERIALIZED(SELECT o.id,d.x FROM hl_input o CROSS JOIN LATERAL(SELECT o.id+1 AS x) d) SELECT * FROM q ORDER BY id",
    "WITH q AS MATERIALIZED(SELECT o.id,d.n FROM hl_input o CROSS JOIN LATERAL(SELECT count(*) AS n FROM hl_input i WHERE i.id<o.id HAVING count(*)>0) d) SELECT * FROM q ORDER BY id",
    "SELECT o.id,d.r FROM hl_input o CROSS JOIN LATERAL(SELECT row_number() OVER(ORDER BY i.id) AS r FROM hl_input i WHERE i.id=o.id) d ORDER BY o.id",
    "SELECT o.id,d.x FROM hl_input o CROSS JOIN LATERAL(SELECT o.id AS x UNION ALL SELECT o.id+10) d ORDER BY o.id,d.x",
    "SELECT o.id,d.x FROM hl_input o CROSS JOIN(SELECT o.id AS x) d",
    "SELECT o.id,d.x FROM hl_input o RIGHT JOIN LATERAL(SELECT o.id AS x) d ON true",
    "SELECT o.id,d.x FROM hl_input o FULL JOIN LATERAL(SELECT o.id AS x) d ON true",
    "SELECT o.id,d.x FROM hl_input o RIGHT JOIN LATERAL(SELECT 9 AS x) d ON false ORDER BY o.id",
    "SELECT o.id,d.x FROM hl_input o FULL JOIN LATERAL(SELECT 9 AS x) d ON false ORDER BY o.id",
    "SELECT d.x FROM LATERAL(SELECT o.id AS x) d CROSS JOIN hl_input o",
    "SELECT o.id,d.x FROM hl_input o CROSS JOIN LATERAL(SELECT o.id AS x) d(y)",
    "SELECT o.id,d.y FROM hl_input o CROSS JOIN LATERAL(SELECT o.id AS x) d(y) ORDER BY o.id",
    "SELECT o.id,(SELECT d.x FROM LATERAL(SELECT o.id AS x) d) FROM hl_input o ORDER BY o.id",
    "SELECT o.id,d.x FROM hl_input o CROSS JOIN LATERAL(SELECT sum(o.id) AS x) d",
    "SELECT setval('hl_seq',1,false)",
    "SELECT o.id,d.x FROM hl_input o CROSS JOIN LATERAL(SELECT nextval('hl_seq')+o.id*0 AS x) d ORDER BY o.id",
    "SELECT currval('hl_seq')",
    "SELECT setval('hl_seq',1,false)",
    "SELECT o.id,d.x FROM hl_input o CROSS JOIN LATERAL(SELECT nextval('hl_seq') AS x) d ORDER BY o.id",
    "SELECT currval('hl_seq')",
    "SELECT o.id,d.x FROM hl_input o CROSS JOIN LATERAL(SELECT 10/(2-o.id) AS x) d LIMIT 1",
    "SELECT o.id,(SELECT r FROM(SELECT o.g AS id) x,(SELECT id AS r) y) FROM hl_input o ORDER BY o.id",
    "SELECT o.id,(SELECT r FROM(SELECT o.g AS id) x,LATERAL(SELECT id AS r) y) FROM hl_input o ORDER BY o.id",
    "SELECT o.id,d.x,e.y FROM hl_input o CROSS JOIN LATERAL(SELECT o.id+1 AS x) d CROSS JOIN LATERAL(SELECT d.x+o.id AS y) e ORDER BY o.id",
    "SELECT o.id,s.v FROM hl_input o CROSS JOIN LATERAL(WITH q AS MATERIALIZED(SELECT o.id AS v) SELECT a.v+b.v AS v FROM q a CROSS JOIN q b) s ORDER BY o.id",
    "SELECT o.id,d.x FROM hl_input o LEFT JOIN hl_input i ON false CROSS JOIN LATERAL(SELECT i.id AS x) d ORDER BY o.id",
    "SELECT p.name,f.attrs FROM(SELECT 'hl_pub'::text AS name) p,pg_get_publication_tables(p.name) f",
    "SELECT p.name,f.attrs FROM(SELECT 'hl_pub'::text AS name) p CROSS JOIN LATERAL pg_get_publication_tables(p.name) f",
    "WITH q AS MATERIALIZED(SELECT p.name,f.attrs FROM(SELECT 'hl_pub'::text AS name) p CROSS JOIN LATERAL pg_get_publication_tables(p.name) f) SELECT * FROM q",
    "SELECT p.name,f.attrs FROM(SELECT 'hl_pub'::text AS name) p RIGHT JOIN pg_get_publication_tables(p.name) f ON true",
    "SELECT p.name,f.attrs FROM(SELECT 'hl_pub'::text AS name) p FULL JOIN LATERAL pg_get_publication_tables(p.name) f ON true",
    "SELECT o.id,d.x FROM hl_input o CROSS JOIN LATERAL(SELECT o.id AS x) d(x,y)",
    "SELECT o.id,d.x,d.z FROM hl_input o CROSS JOIN LATERAL(SELECT o.id AS y,o.id+1 AS z) d(x) ORDER BY o.id",
    "SELECT count(*) FROM hl_input WHERE 1/(id-1)>0 HAVING false",
    "SELECT count(*) FROM hl_input WHERE 1/(id-1)>0 HAVING NULL",
    "SELECT * FROM pg_get_publication_tables(sum(1)::text)",
    "SELECT * FROM pg_get_publication_tables(row_number() OVER()::text)",
    "SELECT * FROM hl_input o RIGHT JOIN LATERAL(WITH q AS(SELECT o.id) SELECT 1) d ON true",
    "SELECT * FROM hl_input o FULL JOIN LATERAL(WITH q AS(SELECT o.id) SELECT 1) d ON true",
    "SELECT o.g,(SELECT count(*) HAVING o.g>1) FROM hl_input o GROUP BY o.g ORDER BY o.g",
    "WITH q AS(SELECT count(*) HAVING 1/0>0) SELECT 1",
    "WITH q AS(SELECT 1 HAVING 1/0>0) SELECT 1",
    "WITH q AS(SELECT count(*) HAVING 1/0>0) SELECT * FROM q",
    "SELECT count(*) HAVING false AND 1/0>0",
    "SELECT count(*) HAVING 1/0>0 AND false",
    "SELECT count(*) HAVING NULL AND 1/0>0",
    "SELECT 1 WHERE false HAVING 1/0>0",
    "SELECT count(*) WHERE false HAVING 1/0>0",
    "SELECT 1 HAVING NOT true AND 1/0>0",
    "SELECT 1 HAVING (true IS FALSE) AND 1/0>0",
    "SELECT 1 HAVING (NULL IS NOT NULL) AND 1/0>0",
    "SELECT 1 HAVING (false OR false) AND 1/0>0",
    "SELECT 1 HAVING lower('X')='y' AND 1/0>0",
    "SELECT 1 HAVING length('')=1 AND 1/0>0",
    "SELECT 1 HAVING coalesce(NULL,false) AND 1/0>0",
    "SELECT 1 HAVING CASE WHEN false THEN true ELSE false END AND 1/0>0",
    "SELECT count(*) FROM hl_input WHERE 1/(id-1)>0 HAVING NOT true",
    "SELECT 1 HAVING true OR 1/0>0",
    "SELECT 1 HAVING ((true OR nextval('hl_seq')>0) IS FALSE) AND 1/0>0",
    "SELECT 1 HAVING ((true OR nextval('hl_seq')>0) IN(false)) AND 1/0>0",
    "SELECT 1 FROM(SELECT 1 AS x) d GROUP BY d.x HAVING d.x IN(1,1/0) AND false",
    "SELECT 1 HAVING ((NULL AND nextval('hl_seq')>0) IS FALSE)",
    "SELECT 1 HAVING coalesce(false,nextval('hl_seq')>0) AND 1/0>0",
    "SELECT 1 HAVING CASE WHEN true THEN false ELSE nextval('hl_seq')>0 END AND 1/0>0",
    "SELECT setval('hl_seq',1,false)",
    "SELECT 1 HAVING ((nextval('hl_seq')>0 OR true) IS FALSE) AND 1/0>0",
    "SELECT nextval('hl_seq')",
    "SELECT setval('hl_seq',1,false)",
    "SELECT 1 HAVING NULL AND nextval('hl_seq')>0",
    "SELECT nextval('hl_seq')",
    "SELECT setval('hl_seq',1,false)",
    "SELECT 1 HAVING nextval('hl_seq')>0 OR true",
    "SELECT nextval('hl_seq')",
]


def check(reference, native, base, socket):
    peers = [QueryConnection(port, "having-lateral") for port in (reference, native)]
    try:
        for sql in SETUP:
            results = [peer.observe(sql) for peer in peers]
            assert results[0] == results[1], (sql, results)
        failures = []
        for index, sql in enumerate(CASES, 1):
            results = [peer.observe(sql) for peer in peers]
            if results[0] != results[1]:
                failures.append((index, sql, results))
                print(f"having-lateral: FAIL {failures[-1]!r}", flush=True)
            else:
                print(f"having-lateral: {index}: PASS", flush=True)
        assert not failures, failures
        print(f"having-lateral: {len(CASES)} C differential cases PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()
    outcomes = [resources(port, native_p, base, socket)
                for port, native_p in ((reference, False), (native, True))]
    assert outcomes[0] == outcomes[1], outcomes
    print("having-lateral: spill, cancellation and same-connection recovery PASS", flush=True)


def resources(port, native, base, socket):
    connection, admin = [QueryConnection(port, "having-lateral-resources") for _ in range(2)]
    pool = ThreadPoolExecutor(max_workers=1)
    old_memory = None
    future = None
    try:
        admin.query("CREATE TABLE hl_big(id int,label text)")
        admin.query("INSERT INTO hl_big VALUES " + ",".join(
            f"({i},'{str(i).zfill(3)}{'x'*93}')" for i in range(300)))
        if native:
            output = control(base, socket, '(format t "HL_MEMORY=~d~%" pg::*query-work-memory-bytes*)')
            old_memory = int(re.search(r"HL_MEMORY=(\d+)", output)[1])
            control(base, socket, '(setf pg::*query-work-memory-bytes* 8192)')
            before = spill_counter(base, socket)
        results = []
        for sql in (
            "SELECT id,count(*) FROM hl_big GROUP BY id HAVING count(*)=1 ORDER BY id",
            "SELECT o.id,d.n FROM hl_input o CROSS JOIN LATERAL(SELECT count(*) AS n FROM hl_big i WHERE i.id>=o.id GROUP BY i.id HAVING count(*)=1 ORDER BY i.id LIMIT 2) d ORDER BY o.id,d.n",
            "WITH q AS MATERIALIZED(SELECT o.id,d.n FROM hl_input o CROSS JOIN LATERAL(SELECT count(*) AS n FROM hl_big i WHERE i.id>=o.id GROUP BY i.id HAVING count(*)=1 ORDER BY i.id LIMIT 2) d) SELECT * FROM q ORDER BY id,n",
            "SELECT o.id,d.n FROM hl_input o CROSS JOIN LATERAL(SELECT sum(10/(299-i.id)) AS n FROM hl_big i WHERE i.id>=o.id HAVING count(*)>0) d",
        ):
            outcome = connection.observe(sql)
            assert outcome[0] == "rows" or outcome == ("error", "22012"), (sql, outcome)
            results.append(outcome)
        if native:
            assert spill_counter(base, socket) > before, "HAVING/LATERAL queries did not spill"
            assert_no_spill_files(base, socket)
            before = spill_counter(base, socket)
        future = pool.submit(connection.observe,
            "WITH q AS MATERIALIZED(SELECT o.id,d.n FROM hl_input o CROSS JOIN LATERAL(SELECT count(*) AS n FROM hl_big a CROSS JOIN hl_big b CROSS JOIN hl_big c WHERE a.id>=o.id HAVING count(*)>0) d) SELECT * FROM q")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert not future.done(), future.result()
            ready = (spill_counter(base, socket) > before if native else
                     admin.query(f"SELECT state FROM pg_stat_activity WHERE pid={connection.backend_pid}") == [["active"]])
            if ready:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("LATERAL query did not start")
        assert admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})") == [["t"]]
        cancelled = future.result(timeout=20)
        assert cancelled == ("error", "57014"), cancelled
        results.append(cancelled)
        if native:
            assert_no_spill_files(base, socket)
        results.append(connection.observe("SELECT 42"))
        return results
    finally:
        if future is not None and not future.done():
            try:
                admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})")
            except Exception:
                pass
        pool.shutdown(wait=True, cancel_futures=True)
        if old_memory is not None:
            control(base, socket, f'(setf pg::*query-work-memory-bytes* {old_memory})')
        for peer in (connection, admin):
            peer.close()


if __name__ == "__main__":
    check(int(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
