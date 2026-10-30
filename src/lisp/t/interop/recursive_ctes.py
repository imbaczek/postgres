"""Recursive working tables, scopes and SQLSTATEs against repository C."""
import sys
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from subqueries import QueryConnection
from subqueries_concurrency import control, spill_counter, assert_no_spill_files

SETUP = (
    "CREATE TABLE rc_edges(a int,b int)",
    "INSERT INTO rc_edges VALUES(1,2),(1,3),(2,4),(3,4),(4,1)",
    "CREATE INDEX rc_edges_a ON rc_edges(a)",
    "CREATE TABLE rc_outer(id int)",
    "INSERT INTO rc_outer VALUES(2),(3),(2)",
    "CREATE SEQUENCE rc_seq",
    "CREATE TABLE rc_target(id int)",
)

MANDELBROT = Path(__file__).with_name("mandelbrot.sql").read_text()

CASES = [
    "SELECT chr(27),chr(233),chr(128512),chr(NULL)",
    "SELECT chr(-1)",
    "SELECT chr(0)",
    "SELECT chr(55296)",
    "SELECT chr(1114112)",
    "SELECT chr(27::smallint)",
    "SELECT chr(27::bigint)",
    "SELECT chr(1,2)",
    "SELECT array_to_string(ARRAY['a',NULL,'b'],','),array_to_string(ARRAY['a',NULL,'b'],',','?')",
    "SELECT array_to_string(ARRAY[1,2,3],':'),array_to_string(ARRAY[true,false],':')",
    "SELECT array_to_string(ARRAY[[1,2],[3,4]],':')",
    "SELECT array_to_string('{}'::text[],','),array_to_string(ARRAY[NULL::text],',')",
    "SELECT array_to_string(ARRAY['a',NULL],',',NULL),array_to_string(NULL::int[],','),array_to_string(ARRAY[1],NULL)",
    "SELECT array_to_string('[0:1]={a,b}'::text[],':')",
    "SELECT array_to_string(ARRAY[1])",
    "SELECT array_to_string(1,',')",
    "SELECT greatest(1,2,NULL),greatest(NULL,NULL),greatest(1::smallint,2::bigint),greatest(1,2.5)",
    "SELECT greatest('b','a'),greatest('NaN'::float8,1::float8)",
    "SELECT greatest(NULL,1/0)",
    "SELECT greatest()",
    "SELECT greatest(1,true)",
    "SELECT pg_catalog.greatest(1,2)",
    "SELECT array_to_string(NULL,',' )",
    "SELECT greatest(DISTINCT 1,2)",
    "SELECT greatest(VARIADIC ARRAY[1,2])",
    "SELECT greatest(a=>1)",
    "SELECT greatest('a'::varchar(3),'b'::varchar(3))",
    "SELECT array_to_string('1 2'::int2vector,',')",
    "VALUES(1,'a')",
    "VALUES(NULL)",
    "VALUES('1') UNION ALL SELECT 1",
    "VALUES(sum(1))",
    "VALUES(row_number() OVER())",
    "VALUES(pg_get_publication_tables(VARIADIC ARRAY[]::text[]))",
    "WITH RECURSIVE r(n) AS(VALUES(0) UNION ALL SELECT n+1 FROM r WHERE n<3) SELECT n FROM r",
    "SELECT (VALUES(7))",
    "SELECT greatest(ALL 1,2)",
    'SELECT "greatest"(1,2)',
    "SELECT chr(DISTINCT 'bad')",
    "SELECT chr('bad' ORDER BY 1)",
    "SELECT least(1,2,NULL),least(NULL,NULL),least(1::smallint,2::bigint),least(1,2.5)",
    "SELECT least('b','a'),least('NaN'::float8,1::float8)",
    "SELECT least('a'::varchar(3),'b'::varchar(3))",
    "SELECT least(NULL,1/0)",
    "SELECT least()",
    "SELECT least(1,true)",
    "SELECT least(ALL 1,2)",
    "SELECT least(DISTINCT 1,2)",
    'SELECT "least"(1,2)',
    "SELECT pg_catalog.least(1,2)",
    "SELECT string_to_array('a,b,c',','),string_to_array(',a,,',',')",
    "SELECT string_to_array('a--b--c','--'),string_to_array('aaaaa','aa')",
    "SELECT string_to_array('abc',''),string_to_array('',''),string_to_array('',','),string_to_array('',NULL)",
    "SELECT string_to_array('aé😀',NULL),string_to_array('é😀é','😀')",
    "SELECT string_to_array('a,NULL,b',',','NULL'),string_to_array(',a,,',',','')",
    "SELECT string_to_array('abc',NULL,'b'),string_to_array('abc','','abc')",
    "SELECT string_to_array(NULL,','),string_to_array('a,b',',',NULL)",
    "SELECT string_to_array('a,b'::varchar,','::varchar)",
    "SELECT string_to_array('abc')",
    "SELECT string_to_array(1,',')",
    "SELECT array_to_string(string_to_array('a,b,c',','),'|')",
    "VALUES(1) ORDER BY column1+1",
    "VALUES(1) ORDER BY (SELECT column1)",
    "WITH q(n) AS (VALUES (7)) VALUES ((SELECT n FROM q))",
    "WITH RECURSIVE q(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM q WHERE n<3) VALUES ((SELECT sum(n) FROM q))",
    "SELECT least(1,2) OVER ()",
    "SELECT greatest(1,2) FILTER (WHERE true)",
    MANDELBROT,
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<100) SELECT sum(n) FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION SELECT n FROM t) SELECT n FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION SELECT 10-n FROM t) SELECT n FROM t ORDER BY n",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t) SELECT n FROM t LIMIT 10",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION SELECT n+1 FROM t) SELECT n FROM t LIMIT 10",
    "WITH RECURSIVE t(n) AS(SELECT 1 WHERE false UNION ALL SELECT n+1 FROM t) SELECT n FROM t",
    "WITH RECURSIVE t(n) AS((SELECT 1 UNION ALL SELECT 1) UNION SELECT n FROM t) SELECT n FROM t",
    "WITH RECURSIVE t(n) AS((SELECT 1 UNION ALL SELECT 1) UNION ALL SELECT n+1 FROM t WHERE n<2) SELECT n FROM t ORDER BY n",
    "WITH RECURSIVE t(n) AS((SELECT 1 UNION SELECT 1) UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT n FROM t ORDER BY n",
    "SELECT 1 UNION SELECT 1 UNION SELECT 2 ORDER BY 1",
    "WITH RECURSIVE t(n) AS(SELECT NULL::int UNION SELECT n FROM t) SELECT n FROM t",
    "WITH RECURSIVE t(n) AS(SELECT '{a,NULL}'::text[] UNION SELECT n FROM t) SELECT n FROM t",
    "WITH RECURSIVE t(n) AS(SELECT sum(1::bigint) UNION SELECT n FROM t) SELECT n FROM t",
    "WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT q.n FROM(SELECT 1 AS i UNION ALL SELECT 2) g CROSS JOIN LATERAL(WITH s AS MATERIALIZED(SELECT n+g.i AS n FROM r WHERE n<2) SELECT n FROM s) q) SELECT n FROM r",
    "SELECT setval('rc_seq',1,false)",
    "WITH RECURSIVE r(n) AS(SELECT 1::int8 UNION ALL(WITH RECURSIVE s(n) AS MATERIALIZED(SELECT n+nextval('rc_seq') FROM r WHERE n<2),t(k) AS(SELECT n FROM s UNION ALL SELECT t.k+s.n FROM t CROSS JOIN s WHERE t.k<3) SELECT k FROM t)) SELECT n FROM r",
    "SELECT currval('rc_seq')",
    "INSERT INTO rc_target VALUES((WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT sum(n)::int4 FROM t))",
    "UPDATE rc_target SET id=(WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<rc_target.id) SELECT sum(n)::int4 FROM t)",
    "SELECT id FROM rc_target",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT t.n+g.i FROM t CROSS JOIN(SELECT 1 AS i UNION ALL SELECT 2) g WHERE t.n<4) SELECT n,count(*) FROM t GROUP BY n ORDER BY n",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION SELECT t.n+g.i FROM t CROSS JOIN(SELECT 1 AS i UNION ALL SELECT 2) g WHERE t.n<4) SELECT n FROM t ORDER BY n",
    "WITH RECURSIVE t(n) AS(SELECT 'a' UNION SELECT 'A' FROM t) SELECT n FROM t ORDER BY n",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT d.n+1 FROM(SELECT n FROM t) d WHERE d.n<5) SELECT n FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL (WITH q AS MATERIALIZED(SELECT n FROM t) SELECT n+1 FROM q WHERE n<5)) SELECT n FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL (WITH q AS NOT MATERIALIZED(SELECT n FROM t) SELECT n+1 FROM q WHERE n<5)) SELECT n FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<4) SELECT a.n,b.n FROM t a CROSS JOIN t b ORDER BY a.n,b.n",
    "WITH RECURSIVE t(n) AS NOT MATERIALIZED(SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<4) SELECT a.n,b.n FROM t a CROSS JOIN t b ORDER BY a.n,b.n",
    "WITH RECURSIVE a AS(SELECT n FROM b),b(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM b WHERE n<3) SELECT n FROM a",
    "WITH RECURSIVE a AS(SELECT 7 AS n),b AS(SELECT n FROM a) SELECT n FROM b",
    "WITH RECURSIVE a AS(SELECT n FROM b),b AS(SELECT 7 AS n) SELECT n FROM a",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<2) SELECT n,(WITH RECURSIVE t(n) AS(SELECT 9 UNION ALL SELECT n+1 FROM t WHERE n<10) SELECT sum(n) FROM t) FROM t ORDER BY n",
    "SELECT id,(WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<o.id) SELECT sum(n) FROM t) FROM rc_outer o ORDER BY id",
    "SELECT o.id,d.s FROM rc_outer o CROSS JOIN LATERAL(WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<o.id) SELECT sum(n) AS s FROM t) d ORDER BY o.id",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION SELECT e.b FROM t JOIN rc_edges e ON e.a=t.n) SELECT n FROM t ORDER BY n",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<5) SELECT n,row_number() OVER(ORDER BY n),sum(n) OVER(ORDER BY n) FROM t ORDER BY n",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<5) SELECT sum(n) FROM t HAVING count(*)=5",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<20000) SELECT count(*) FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT 10/(2-n) FROM t) SELECT n FROM t LIMIT 1",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t) SELECT 1 WHERE false",
    "WITH RECURSIVE t(n) AS(SELECT 1/0 UNION ALL SELECT n+1 FROM t) SELECT 1",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n::bigint FROM t) SELECT * FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n,n FROM t) SELECT * FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 'a'::varchar(2) UNION ALL SELECT n::varchar(3) FROM t) SELECT * FROM t",
    "WITH RECURSIVE t(n) AS(SELECT NULL UNION ALL SELECT 1 FROM t) SELECT * FROM t",
    "WITH RECURSIVE t(n) AS(SELECT n FROM t UNION ALL SELECT 1) SELECT * FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT a.n FROM t a CROSS JOIN t b) SELECT * FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT (SELECT n FROM t)) SELECT * FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT sum(n) FROM t) SELECT * FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT count(*) FROM t) SELECT * FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT t.n FROM rc_outer o LEFT JOIN t ON true) SELECT * FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT t.n FROM t RIGHT JOIN rc_outer o ON true) SELECT * FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT t.n FROM t FULL JOIN rc_outer o ON true) SELECT * FROM t",
    "WITH RECURSIVE t(n) AS(SELECT n+1 FROM t) SELECT * FROM t",
    "WITH RECURSIVE a(n) AS(SELECT n FROM b),b(n) AS(SELECT n FROM a) SELECT * FROM a",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t LIMIT 2) SELECT * FROM t",
    "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t ORDER BY n) SELECT * FROM t",
    "SELECT setval('rc_seq',1,false)",
    "WITH RECURSIVE t(n,x) AS(SELECT 1,(SELECT nextval('rc_seq')) UNION ALL SELECT n+1,(SELECT nextval('rc_seq')) FROM t WHERE n<3) SELECT * FROM t ORDER BY n",
    "SELECT currval('rc_seq')",
]


def check(reference, native, base, socket):
    peers = [QueryConnection(port, "recursive-ctes") for port in (reference, native)]
    try:
        for sql in SETUP:
            outcomes = [peer.observe(sql) for peer in peers]
            assert outcomes[0] == outcomes[1], (sql, outcomes)
        failures = []
        for index, sql in enumerate(CASES, 1):
            outcomes = [peer.observe(sql) for peer in peers]
            if sql == MANDELBROT:
                for outcome in outcomes:
                    assert outcome[0] == "rows", outcome
                    assert len(outcome[2]) == 102, len(outcome[2])
                    assert all(len(re.findall(r"\x1b\[48;5;\d+m \x1b\[0m", row[0])) == 102
                               for row in outcome[2]), "incomplete Mandelbrot image"
            if outcomes[0] != outcomes[1]:
                failures.append((index, sql, outcomes))
                print(f"recursive-ctes: FAIL {failures[-1]!r}", flush=True)
            else:
                print(f"recursive-ctes: {index}: PASS", flush=True)
        assert not failures, failures
        print(f"recursive-ctes: {len(CASES)} C differential cases PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()
    outcomes = [resources(port, native_p, base, socket)
                for port, native_p in ((reference, False), (native, True))]
    assert outcomes[0] == outcomes[1], outcomes
    print("recursive-ctes: spill, cancellation, temp quota and same-connection recovery PASS", flush=True)


def resources(port, native, base, socket):
    connection, admin = [QueryConnection(port, "recursive-cte-resources") for _ in range(2)]
    pool = ThreadPoolExecutor(max_workers=1)
    future = None
    old_memory = None
    old_limit = None
    try:
        admin.query("CREATE TABLE rc_wide(id int,label text)")
        admin.query("INSERT INTO rc_wide VALUES " + ",".join(
            f"({i},'{str(i).zfill(3)}{'x'*93}')" for i in range(300)))
        if native:
            output = control(base, socket, '(format t "RC_MEMORY=~d~%" pg::*query-work-memory-bytes*)')
            old_memory = int(re.search(r"RC_MEMORY=(\d+)", output)[1])
            output = control(base, socket, '(format t "RC_LIMIT=~d~%" (or pg::*query-temp-file-limit-bytes* -1))')
            old_limit = int(re.search(r"RC_LIMIT=(-?\d+)", output)[1])
            control(base, socket, '(setf pg::*query-work-memory-bytes* 8192)')
            before = spill_counter(base, socket)
        outcomes = []
        for sql in (
            "WITH RECURSIVE t(n,s) AS(SELECT 1,'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'::text UNION ALL SELECT n+1,s FROM t WHERE n<400) SELECT count(*) FROM t a CROSS JOIN t b",
            "WITH RECURSIVE t(n) AS(SELECT 1 UNION SELECT CASE WHEN n<1500 THEN n+1 ELSE 1 END FROM t) SELECT sum(n) FROM t",
            "WITH RECURSIVE t(n) AS(SELECT 1 UNION SELECT n FROM t) SELECT * FROM t",
            "WITH RECURSIVE t(n,s) AS(SELECT id,label FROM rc_wide UNION ALL SELECT n+300,s FROM t WHERE n<300) SELECT count(*) FROM t",
            "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<400) SELECT 10/(399-n) FROM t",
        ):
            outcome = connection.observe(sql)
            assert outcome[0] == "rows" or outcome == ("error", "22012"), (sql, outcome)
            outcomes.append(outcome)
        if native:
            assert spill_counter(base, socket) > before, "recursive query did not spill"
            assert_no_spill_files(base, socket)
            before = spill_counter(base, socket)
        future = pool.submit(connection.observe,
            "WITH RECURSIVE t(n) AS(SELECT 1 UNION SELECT n+1 FROM t) SELECT count(*) FROM t")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert not future.done(), future.result()
            ready = (spill_counter(base, socket) > before if native else
                     admin.query(f"SELECT state FROM pg_stat_activity WHERE pid={connection.backend_pid}") == [["active"]])
            if ready:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("recursive query did not start")
        assert admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})") == [["t"]]
        cancelled = future.result(timeout=20)
        assert cancelled == ("error", "57014"), cancelled
        outcomes.append(cancelled)
        if native:
            assert_no_spill_files(base, socket)
            control(base, socket, '(setf pg::*query-temp-file-limit-bytes* 16384)')
        else:
            connection.query("SET temp_file_limit=16")
        quota = connection.observe(
            "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t) SELECT count(*) FROM t")
        assert quota == ("error", "53400"), quota
        outcomes.append(quota)
        if native:
            assert_no_spill_files(base, socket)
            control(base, socket, f'(setf pg::*query-temp-file-limit-bytes* {"nil" if old_limit == -1 else old_limit})')
        else:
            connection.query("SET temp_file_limit=-1")
        outcomes.append(connection.observe("SELECT 42"))
        return outcomes
    finally:
        if future is not None and not future.done():
            try:
                admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})")
            except Exception:
                pass
        pool.shutdown(wait=True, cancel_futures=True)
        if old_memory is not None:
            control(base, socket, f'(setf pg::*query-work-memory-bytes* {old_memory})')
        if old_limit is not None:
            control(base, socket, f'(setf pg::*query-temp-file-limit-bytes* {"nil" if old_limit == -1 else old_limit})')
        for peer in (connection, admin):
            peer.close()


if __name__ == "__main__":
    check(int(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
