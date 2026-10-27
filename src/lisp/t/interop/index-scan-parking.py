"""Park a spilled index cursor across replay, promotion, reload and later splits."""
import os
import re
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg2

MATCHING = 96
MATCH_LABEL = "before" + "x" * 80
PAD_COLUMNS = 24
LATER_INSERTS = 1024


def connect(port):
    connection = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                  dbname="postgres", connect_timeout=3,
                                  application_name="index-parking")
    connection.autocommit = True
    return connection


def query(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall() if cursor.description else None


def scalar(connection, sql):
    return query(connection, sql)[0][0]


def wait_until(predicate):
    deadline = time.monotonic() + 30
    while not predicate():
        assert time.monotonic() < deadline, "index parking barrier timed out"
        time.sleep(0.05)


def control(expression):
    result = subprocess.run(
        [os.environ.get("SBCL", "sbcl"), "--script", str(base / "t/control-client.lisp"),
         "unix", str(root / "target.sock")],
        input=expression + "\n(quit)\n", text=True, capture_output=True,
        timeout=30, check=True)
    assert ";; error:" not in result.stdout + result.stderr, result.stdout + result.stderr
    return result.stdout


def index_blocks():
    output = control('(format t "INDEX_BLOCKS=~d~%" (index-parking-index-blocks))')
    return int(re.search(r"INDEX_BLOCKS=(\d+)", output)[1])


def setup(port):
    connection = connect(port)
    try:
        query(connection, "SET statement_timeout = '30s'")
        pads = ",".join(f"pad{i} varchar(120)" for i in range(PAD_COLUMNS))
        query(connection, "CREATE TABLE promotion_index_items "
              f"(id integer PRIMARY KEY,label varchar(120),{pads})")
        query(connection, "INSERT INTO promotion_index_items (id,label) VALUES " +
              ",".join(f"({i},'{MATCH_LABEL}')" for i in range(1, MATCHING + 1)))
        padding = ",".join("'" + "x" * 120 + "'" for _ in range(PAD_COLUMNS))
        for start in range(1000, 5096, 64):
            query(connection, "INSERT INTO promotion_index_items VALUES " +
                  ",".join(f"({i},'other',{padding})" for i in range(start, start + 64)))
        query(connection, "CREATE INDEX promotion_index_label_idx ON promotion_index_items (label)")
        query(connection, "ANALYZE promotion_index_items")
        plan = query(connection, "EXPLAIN SELECT id,label FROM promotion_index_items "
                     f"WHERE label='{MATCH_LABEL}'")
        assert any("Index Scan" in row[0] for row in plan), plan
        print("index-scan-parking: indexed fixture prepared before base backup")
    finally:
        connection.close()


if len(sys.argv) > 1 and sys.argv[1] == "--setup":
    setup(int(sys.argv[2]))
    sys.exit(0)


base, root = Path(sys.argv[1]), Path(sys.argv[2])
source_port, target_port, target_pid = map(int, sys.argv[3:])
source, target, reader = connect(source_port), connect(target_port), connect(target_port)
pool = ThreadPoolExecutor(max_workers=1)
matching, later_inserts = MATCHING, LATER_INSERTS
try:
    query(source, "INSERT INTO promotion_index_items (id,label) VALUES (60000,'warm')")
    query(source, "INSERT INTO promotion_index_items (id,label) VALUES (60001,'warm')")
    wait_until(lambda: scalar(target, "SELECT count(*) FROM promotion_index_items") == matching + 4098)
    explain = f"EXPLAIN SELECT id,label FROM promotion_index_items WHERE label='{MATCH_LABEL}'"
    plan = query(source, explain)
    assert any("Index Scan" in row[0] for row in plan), plan
    wait_until(lambda: any("Index Scan" in row[0] for row in query(target, explain)))
    query(reader, "SELECT set_config('pg_lisp.parking_effects','scan',false)")
    query(reader, "SET statement_timeout = '120s'")
    query(reader, "BEGIN ISOLATION LEVEL REPEATABLE READ")
    before = query(reader, f"SELECT id,label FROM promotion_index_items WHERE label='{MATCH_LABEL}'")
    assert len(before) == matching
    control(f'(load "{base}/t/live-operation-parking-native.lisp")')
    control(f'(load "{base}/t/index-scan-parking-native.lisp")')
    control(f'(index-parking-install #p"{root}/" {matching})')
    future = pool.submit(query, reader, f"""
        SELECT id,label,
               set_config('pg_lisp.parking_effects',current_setting('pg_lisp.parking_effects') || ':b',false),
               current_setting('pg_lisp_test_index_park'),
               set_config('pg_lisp.parking_effects',current_setting('pg_lisp.parking_effects') || ':a',false)
          FROM promotion_index_items WHERE label='{MATCH_LABEL}'
    """)
    wait_until(lambda: (root / "parked").exists() or future.done())
    if future.done():
        raise AssertionError(f"index expression completed before parking: {future.result()}")
    assert int((root / "parked").read_text()) == target_pid
    paths = [Path(line) for line in (root / "spools").read_text().splitlines()]
    assert paths and all(path.exists() and path.resolve().is_relative_to(root / "copy") for path in paths)
    os.kill(target_pid, signal.SIGHUP)
    wait_until(lambda: (root / "reload-backed-off").exists())
    query(source, "UPDATE promotion_index_items SET label='parent-after' WHERE id=3")
    query(source, f"INSERT INTO promotion_index_items (id,label) VALUES (50000,'{MATCH_LABEL}')")
    position = scalar(source, "SELECT pg_current_wal_flush_lsn()")
    wait_until(lambda: scalar(target, f"SELECT pg_last_wal_receive_lsn() >= '{position}'::pg_lsn"))
    wait_until(lambda: scalar(target, f"SELECT pg_last_wal_replay_lsn() >= '{position}'::pg_lsn"))
    result = subprocess.run(
        [str(base / "bin/pg_ctl"), "promote", "-D", str(root / "copy"), "-w", "-t", "30"],
        text=True, capture_output=True, timeout=40)
    assert result.returncode == 0, result.stdout + result.stderr
    assert not future.done(), "index query resumed before its release"
    assert scalar(target, "SELECT pg_is_in_recovery()") is False
    assert scalar(target, "SELECT label FROM promotion_index_items WHERE id=3") == "parent-after"
    assert scalar(target, "SELECT label FROM promotion_index_items WHERE id=50000") == MATCH_LABEL
    blocks = index_blocks()
    query(target, "SET statement_timeout = '15s'")
    query(target, "UPDATE promotion_index_items SET label='child-after' WHERE id=2")
    query(target, "INSERT INTO promotion_index_items (id,label) VALUES " +
          ",".join(f"({i},'{MATCH_LABEL}')" for i in range(100000, 100000 + later_inserts)))
    assert index_blocks() > blocks, "concurrent writer did not split the index"
    (root / "release").touch()
    actual = future.result(timeout=30)
    assert [(row[0], row[1]) for row in actual] == before
    for index, row in enumerate(actual):
        prefix = "scan" + ":b:a" * index
        assert row[2:] == (prefix + ":b", "resumed" if index == 0 else "visited", prefix + ":b:a"), row
    assert all(not path.exists() for path in paths), "detached spool survived query cleanup"
    wait_until(lambda: ":NEW" in control("(if (parking-native-reloaded-p) :new :old)"))
    control(f"(assert (index-parking-verify {matching}))")
    assert sorted(query(reader, f"SELECT id,label FROM promotion_index_items WHERE label='{MATCH_LABEL}'")) == sorted(before)
    assert scalar(reader, "SHOW transaction_read_only") == "on"
    query(reader, "COMMIT")
    assert scalar(reader, "SHOW transaction_read_only") == "off"
    assert scalar(reader, f"SELECT count(*) FROM promotion_index_items WHERE label='{MATCH_LABEL}'") == matching + later_inserts - 1
    print("index-scan-parking: spilled tail, replay, promotion, splits, exactly-once effects and reload PASS")
finally:
    (root / "release").touch()
    reader.close()
    pool.shutdown(wait=True)
    source.close()
    target.close()
