"""Exercise admission parking through an owned-server test hook, not pg_promote."""
import os
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg2


def connect(port):
    connection = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                  dbname="postgres", connect_timeout=3,
                                  application_name="native-parking")
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
        assert time.monotonic() < deadline, "native parking barrier timed out"
        time.sleep(0.05)


def control(expression):
    result = subprocess.run(
        [os.environ.get("SBCL", "sbcl"), "--script", str(base / "t/control-client.lisp"),
         "unix", str(root / "target.sock")],
        input=expression + "\n(quit)\n", text=True, capture_output=True,
        timeout=30, check=True)
    assert ";; error:" not in result.stdout + result.stderr, result.stdout + result.stderr
    return result.stdout


base, root = Path(sys.argv[1]), Path(sys.argv[2])
source_port, target_port, target_pid = map(int, sys.argv[3:])
source, target, reader = connect(source_port), connect(target_port), connect(target_port)
pool = ThreadPoolExecutor(max_workers=1)
try:
    query(source, "INSERT INTO promotion_sql_items VALUES (2,'warm')")
    wait_until(lambda: scalar(target, "SELECT count(*) FROM promotion_sql_items") == 2)
    query(reader, "SELECT set_config('pg_lisp.parking_effects','native-parking',false)")
    query(reader, "BEGIN ISOLATION LEVEL REPEATABLE READ")
    before = [(1, "before"), (2, "warm")]
    assert query(reader, "SELECT id,label FROM promotion_sql_items ORDER BY id") == before
    control(f'(load "{base}/t/live-operation-parking-native.lisp")')
    control(f'(parking-native-install #p"{root}/")')
    future = pool.submit(query, reader, """
        SELECT set_config('pg_lisp.parking_effects',current_setting('pg_lisp.parking_effects') || ':before',false),
               current_setting('pg_lisp_test_park'),
               set_config('pg_lisp.parking_effects',current_setting('pg_lisp.parking_effects') || ':after',false)
    """)
    wait_until(lambda: (root / "parked").exists() or future.done())
    if future.done():
        raise AssertionError(f"parking expression completed before its hook: {future.result()}")
    assert int((root / "parked").read_text()) == target_pid
    os.kill(target_pid, signal.SIGHUP)
    wait_until(lambda: (root / "reload-backed-off").exists())
    assert int((root / "reload-backed-off").read_text()) == target_pid
    query(source, "UPDATE promotion_sql_items SET label='after' WHERE id=1")
    query(source, "INSERT INTO promotion_sql_items VALUES (3,'new')")
    position = scalar(source, "SELECT pg_current_wal_flush_lsn()")
    wait_until(lambda: scalar(target, f"SELECT pg_last_wal_receive_lsn() >= '{position}'::pg_lsn"))
    result = subprocess.run(
        [str(base / "bin/pg_ctl"), "promote", "-D", str(root / "copy"), "-w", "-t", "30"],
        text=True, capture_output=True, timeout=40)
    assert result.returncode == 0, result.stdout + result.stderr
    assert not future.done(), "parked expression completed before its release"
    assert scalar(target, "SELECT pg_is_in_recovery()") is False
    (root / "release").touch()
    assert future.result(timeout=30) == [
        ("native-parking:before", "resumed", "native-parking:before:after")]
    wait_until(lambda: ":NEW" in control("(if (parking-native-reloaded-p) :new :old)"))
    control("(assert (parking-native-verify))")
    assert query(reader, "SELECT id,label FROM promotion_sql_items ORDER BY id") == before
    assert scalar(reader, "SHOW transaction_read_only") == "on"
    query(reader, "COMMIT")
    assert scalar(reader, "SHOW transaction_read_only") == "off"
    assert query(reader, "SELECT id,label FROM promotion_sql_items ORDER BY id") == [
        (1, "after"), (2, "warm"), (3, "new")]
    query(reader, "INSERT INTO promotion_sql_items VALUES (4,'primary')")
    print("live-operation-parking: real SQL stack, exactly-once effects, snapshots and reload PASS")
finally:
    (root / "release").touch()
    reader.close()
    pool.shutdown(wait=True)
    source.close()
    target.close()
