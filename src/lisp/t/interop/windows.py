"""Window SQL and resource behavior against the repository C implementation."""
import re
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from subqueries import QueryConnection
from subqueries_concurrency import control, spill_counter, assert_no_spill_files


SETUP = (
    "CREATE TABLE win_input(id int,g int,v int,label varchar(20))",
    "INSERT INTO win_input VALUES(1,1,10,'a'),(2,1,20,'b'),(3,1,20,'c'),(4,2,NULL,NULL),(5,2,5,'e'),(6,NULL,7,'f')",
    "CREATE SEQUENCE win_seq",
)

CASES = [
    "SELECT row_number() OVER (),rank() OVER (),dense_rank() OVER ()",
    "SELECT row_number() OVER () WHERE false",
    "SELECT count(*) OVER () WHERE false",
    "SELECT count(*) OVER (),sum(v) OVER () FROM win_input ORDER BY id",
    "SELECT id,rank() OVER (PARTITION BY g ORDER BY v),dense_rank() OVER (PARTITION BY g ORDER BY v) FROM win_input ORDER BY id",
    "SELECT id,row_number() OVER (PARTITION BY g ORDER BY v,id) FROM win_input ORDER BY id",
    "SELECT id,sum(v) OVER (ORDER BY v DESC),rank() OVER (ORDER BY v DESC) FROM win_input ORDER BY id",
    "SELECT id,sum(v) OVER (ORDER BY v),sum(v) OVER (ORDER BY id DESC) FROM win_input ORDER BY id",
    "SELECT id,count(*) OVER w,sum(v) OVER w FROM win_input WINDOW w AS (PARTITION BY g ORDER BY v) ORDER BY id",
    "SELECT id,rank() OVER (w ORDER BY v) FROM win_input WINDOW w AS (PARTITION BY g) ORDER BY id",
    "SELECT id,count(*) OVER w FROM win_input WINDOW w AS (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY id",
    "SELECT id,rank() OVER w2 FROM win_input WINDOW w AS (PARTITION BY g),w2 AS (w ORDER BY v) ORDER BY id",
    "SELECT sum(sum(v)) OVER (),count(*) OVER () FROM win_input GROUP BY g ORDER BY g",
    "SELECT g,rank() OVER (ORDER BY sum(v)) FROM win_input GROUP BY g ORDER BY g",
    "SELECT DISTINCT rank() OVER (ORDER BY g) FROM win_input ORDER BY rank() OVER (ORDER BY g)",
    "SELECT id FROM win_input ORDER BY row_number() OVER (ORDER BY id DESC)",
    "SELECT (SELECT sum(i.v) OVER () FROM win_input i WHERE i.id=o.id) FROM win_input o ORDER BY o.id",
    "SELECT (SELECT sum(o.v) OVER ()) FROM win_input o ORDER BY o.id",
    "SELECT EXISTS(SELECT rank() OVER (ORDER BY 1/0) FROM win_input)",
    "SELECT 1 WINDOW w AS (ORDER BY missing)",
    "SELECT row_number() OVER (ORDER BY 1)",
    "SELECT row_number() OVER (ORDER BY alias) AS alias",
    "SELECT 1 WHERE row_number() OVER ()=1",
    "SELECT sum(row_number() OVER ())",
    "SELECT sum(rank() OVER ()) OVER ()",
    "SELECT rank() OVER (ORDER BY row_number() OVER ())",
    "SELECT 1 GROUP BY row_number() OVER ()",
    "SELECT rank() OVER w WINDOW w AS (w)",
    "SELECT rank() OVER w WINDOW w AS (),w AS ()",
    "SELECT rank() OVER (w) WINDOW w AS (ROWS UNBOUNDED PRECEDING)",
    "SELECT rank() OVER (w PARTITION BY g) FROM win_input WINDOW w AS ()",
    "SELECT rank() OVER (w ORDER BY v) FROM win_input WINDOW w AS (ORDER BY id)",
    "SELECT rank() OVER ()+1",
    "SELECT count(*) OVER ()::text",
    "SELECT setval('win_seq',1,false)",
    "SELECT sum(nextval('win_seq')) OVER () FROM win_input LIMIT 1",
    "SELECT currval('win_seq')",
    "SELECT setval('win_seq',1,false)",
    "SELECT sum(nextval('win_seq')) OVER (ROWS UNBOUNDED PRECEDING) FROM win_input LIMIT 1",
    "SELECT currval('win_seq')",
    "SELECT setval('win_seq',1,false)",
    "SELECT row_number() OVER (ORDER BY nextval('win_seq')),rank() OVER (ORDER BY nextval('win_seq')) FROM win_input ORDER BY id",
    "SELECT currval('win_seq')",
    "SELECT setval('win_seq',1,false)",
    "SELECT sum(nextval('win_seq')) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING),sum(nextval('win_seq')) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM win_input ORDER BY id",
    "SELECT currval('win_seq')",
    "SELECT setval('win_seq',1,false)",
    "SELECT sum(nextval('win_seq')) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS s FROM win_input ORDER BY sum(nextval('win_seq')) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING)",
    "SELECT currval('win_seq')",
    "SELECT setval('win_seq',1,false)",
    "SELECT 1 FROM win_input WINDOW w AS(ORDER BY nextval('win_seq'))",
    "SELECT currval('win_seq')",
    "SELECT 1 FROM win_input WINDOW w AS(ORDER BY 1/0)",
]

CASES += [
    "SELECT setval('win_seq',1,false)",
    "SELECT DISTINCT sum(nextval('win_seq')) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM win_input ORDER BY sum(nextval('win_seq')) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING)",
    "SELECT currval('win_seq')",
    "SELECT setval('win_seq',1,false)",
    "SELECT sum(nextval('win_seq')) OVER (ORDER BY v) FROM win_input LIMIT 1",
    "SELECT currval('win_seq')",
    "SELECT setval('win_seq',1,false)",
    "SELECT row_number() OVER () FROM (SELECT nextval('win_seq') AS x FROM win_input) d LIMIT 1",
    "SELECT currval('win_seq')",
    "SELECT id,rank() OVER (PARTITION BY (SELECT g) ORDER BY (SELECT v)) FROM win_input ORDER BY id",
    "SELECT g,sum(v) OVER () FROM win_input GROUP BY g",
    "SELECT rank() OVER (ORDER BY v) FROM win_input GROUP BY g",
    "SELECT row_number() OVER (ORDER BY pg_get_publication_tables('unused'))",
    "SELECT rank() OVER w FROM win_input WINDOW w AS (ORDER BY v),unused AS (PARTITION BY missing)",
]

for sql in (
    "SELECT nextval('win_seq') AS n,row_number() OVER(ORDER BY nextval('win_seq')) FROM win_input ORDER BY n",
    "SELECT nextval('win_seq')+1 AS n,row_number() OVER(ORDER BY nextval('win_seq')) FROM win_input ORDER BY n",
    "SELECT id,sum(v) OVER(ORDER BY nextval('win_seq') ROWS UNBOUNDED PRECEDING),sum(v) OVER(ORDER BY nextval('win_seq') ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM win_input ORDER BY id",
    "SELECT sum(nextval('win_seq')) OVER(ORDER BY nextval('win_seq')) FROM win_input ORDER BY id",
    "SELECT sum(nextval('win_seq')+1) OVER(ORDER BY nextval('win_seq')) FROM win_input ORDER BY id",
    "SELECT rank() OVER(PARTITION BY nextval('win_seq') ORDER BY nextval('win_seq')) FROM win_input ORDER BY id",
    "SELECT sum(nextval('win_seq')),rank() OVER(ORDER BY sum(nextval('win_seq'))) FROM win_input",
    "SELECT rank() OVER(ORDER BY sum(nextval('win_seq'))),sum(nextval('win_seq')) FROM win_input",
    "SELECT sum(nextval('win_seq')) AS a,sum(nextval('win_seq')) AS b,rank() OVER(ORDER BY sum(nextval('win_seq'))) FROM win_input",
    "SELECT sum(nextval('win_seq')) AS a,rank() OVER w FROM win_input WINDOW w AS(ORDER BY sum(nextval('win_seq')))",
    "SELECT (SELECT nextval('win_seq')+o.id*0)+1,row_number() OVER(ORDER BY (SELECT nextval('win_seq')+o.id*0)) FROM win_input o ORDER BY o.id",
    "SELECT sum((SELECT nextval('win_seq')::int4+o.id*0)) OVER(ORDER BY (SELECT nextval('win_seq')::int4+o.id*0)) FROM win_input o ORDER BY o.id",
    "SELECT (SELECT nextval('win_seq')+o.id*0) AS n,row_number() OVER(ORDER BY (SELECT nextval('win_seq')+o.id*0)) FROM win_input o ORDER BY o.id",
    "SELECT sum(sum(nextval('win_seq')::int4)) OVER(),sum(sum(nextval('win_seq')::int4)) OVER() FROM win_input",
):
    CASES.extend(("SELECT setval('win_seq',1,false)", sql, "SELECT currval('win_seq')"))

CASES += [
    'SELECT row_number() OVER ("partition") WINDOW "partition" AS ()',
    'SELECT row_number() OVER "order" WINDOW "order" AS ()',
    "SELECT row_number()",
    "SELECT row_number(1)",
    "SELECT length('x') filter",
    "SELECT length('x') AS filter",
    'SELECT length(\'x\') "filter"',
    "SELECT filter.id FROM win_input filter ORDER BY filter.id",
    "SELECT length('x') OVER ()",
    "SELECT no_such_function() OVER ()",
    "SELECT setval('win_seq',1,false)",
    "SELECT row_number() OVER(ORDER BY 1) FROM (SELECT nextval('win_seq') AS x FROM win_input) d LIMIT 1",
    "SELECT currval('win_seq')",
    "SELECT setval('win_seq',1,false)",
    "SELECT row_number() OVER(ORDER BY (SELECT 1)) FROM (SELECT nextval('win_seq') AS x FROM win_input) d LIMIT 1",
    "SELECT currval('win_seq')",
    "SELECT sum(DISTINCT v) OVER() FROM win_input",
]

for mode in ("ROWS", "RANGE"):
    for tail in ("CURRENT ROW", "UNBOUNDED FOLLOWING"):
        for expression in ("sum(v)", "count(*)", "array_agg(v::text)"):
            ordering = "v,id" if mode == "ROWS" else "v"
            CASES.append(f"SELECT id,{expression} OVER (PARTITION BY g ORDER BY {ordering} {mode} BETWEEN UNBOUNDED PRECEDING AND {tail}) FROM win_input ORDER BY id")

rng = random.Random(742103)
for trial in range(30):
    rows = [(i, rng.choice(("1", "2", "NULL::int4")), rng.choice(("-3", "0", "2", "2", "NULL::int4")))
            for i in range(1, rng.randrange(2, 10))]
    source = " UNION ALL ".join(f"SELECT {i} AS id,{g} AS g,{v} AS v" for i, g, v in rows)
    mode = rng.choice(("ROWS", "RANGE"))
    tail = rng.choice(("CURRENT ROW", "UNBOUNDED FOLLOWING"))
    ordering = "v,id" if mode == "ROWS" else "v"
    CASES.append(f"SELECT id,sum(v) OVER(PARTITION BY g ORDER BY {ordering} {mode} BETWEEN UNBOUNDED PRECEDING AND {tail}),rank() OVER(PARTITION BY g ORDER BY v),dense_rank() OVER(PARTITION BY g ORDER BY v) FROM ({source}) d ORDER BY id")


def check(reference_port, native_port, base, socket):
    peers = [QueryConnection(reference_port, "windows"), QueryConnection(native_port, "windows")]
    try:
        for sql in SETUP:
            results = [peer.observe(sql) for peer in peers]
            assert results[0] == results[1], (sql, results)
        for index, sql in enumerate(CASES, 1):
            results = [peer.observe(sql) for peer in peers]
            assert results[0] == results[1], (index, sql, results)
            print(f"windows: {index}: PASS", flush=True)
        print(f"windows: {len(CASES)} C differential cases PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()
    results = [resources(port, native, base, socket)
               for port, native in ((reference_port, False), (native_port, True))]
    assert results[0] == results[1], results
    print("windows: forced spill, cancellation and same-connection recovery PASS", flush=True)


def resources(port, native, base, socket):
    connection, admin = [QueryConnection(port, "window-resources") for _ in range(2)]
    old_memory = None
    future = None
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        admin.query("CREATE TABLE win_big(id int)")
        admin.query("INSERT INTO win_big VALUES " + ",".join(f"({i})" for i in range(300)))
        if native:
            output = control(base, socket, '(format t "WINDOW_MEMORY=~d~%" pg::*query-work-memory-bytes*)')
            old_memory = int(re.search(r"WINDOW_MEMORY=(\d+)", output)[1])
            control(base, socket, '(setf pg::*query-work-memory-bytes* 8192)')
            before = spill_counter(base, socket)
        observation = connection.observe(
            "SELECT id,sum(id) OVER (ORDER BY id),array_agg(id::text) OVER () FROM win_big ORDER BY id LIMIT 3")
        assert observation[0] == "rows", observation
        if native:
            assert spill_counter(base, socket) > before, "window query did not spill"
            assert_no_spill_files(base, socket)
            before = spill_counter(base, socket)
        future = pool.submit(connection.observe,
                             "SELECT sum(a.id) OVER () FROM win_big a CROSS JOIN win_big b CROSS JOIN win_big c LIMIT 1")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert not future.done(), future.result()
            ready = (spill_counter(base, socket) > before if native else
                     admin.query(f"SELECT state FROM pg_stat_activity WHERE pid={connection.backend_pid}") == [["active"]])
            if ready:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("window work did not start")
        assert admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})") == [["t"]]
        cancelled = future.result(timeout=20)
        assert cancelled == ("error", "57014"), cancelled
        after = connection.observe("SELECT row_number() OVER ()")
        if native:
            assert_no_spill_files(base, socket)
        return observation, cancelled, after
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
