import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import psycopg2


def connect(port):
    connection = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                  dbname="postgres", connect_timeout=3)
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
        assert time.monotonic() < deadline, "promotion reader barrier timed out"
        time.sleep(0.05)


def control(expression):
    result = subprocess.run(
        [os.environ.get("SBCL", "sbcl"), "--script", str(base / "t/control-client.lisp"),
         "unix", str(root / "target.sock")],
        input=expression + "\n(quit)\n", text=True, capture_output=True,
        timeout=30, check=True)
    assert ";; error:" not in result.stdout + result.stderr, result.stdout + result.stderr
    return result.stdout


def only_snapshot():
    reader = connect(target_port)
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        pid = scalar(reader, "SELECT pg_backend_pid()")
        query(reader, "BEGIN ISOLATION LEVEL REPEATABLE READ")
        query(reader, "SELECT 1")
        control(f'(load "{base}/t/promotion-readers-native.lisp")')
        wrap = bool(os.environ.get("PROMOTION_WRAP"))
        control(f'(promotion-reader-test-install-single {pid} {"t" if wrap else "nil"})')
        query(source, "UPDATE promotion_sql_items SET label='after' WHERE id=1")
        query(source, "INSERT INTO promotion_sql_items VALUES (3,'new')")
        position = scalar(source, "SELECT pg_current_wal_flush_lsn()")
        wait_until(lambda: scalar(target, f"SELECT pg_last_wal_receive_lsn() >= '{position}'::pg_lsn"))
        result = subprocess.run(
            [str(base / "bin/pg_ctl"), "promote", "-D", str(root / "copy"), "-w", "-t", "30"],
            text=True, capture_output=True, timeout=40)
        assert result.returncode == 0, result.stdout + result.stderr
        if os.environ.get("PROMOTION_DEBUG_PAUSE"):
            print(f"promotion-readers: debug root={root} target_pid={target_pid}", flush=True)
            deadline = time.monotonic() + 300
            while not (root / "debug-release").exists():
                assert time.monotonic() < deadline, "debug release timed out"
                time.sleep(0.05)

        def write():
            writer = connect(target_port)
            try:
                query(writer, "INSERT INTO promotion_sql_items VALUES (4,'primary')")
            finally:
                writer.close()

        future = pool.submit(write)
        if wrap:
            wait_until(lambda: ":PENDING" in control("(if *xid-wrap-pending* :pending :waiting)"))
            assert not future.done(), "wrap reused XIDs while the old snapshot remained"
        assert query(reader, "SELECT id,label FROM promotion_sql_items ORDER BY id") == [(1, "before"), (2, "warm")]
        assert scalar(reader, "SHOW transaction_read_only") == "on"
        query(reader, "COMMIT")
        future.result(timeout=30)
        assert scalar(reader, "SHOW transaction_read_only") == "off"
        query(reader, "INSERT INTO promotion_sql_items VALUES (5,'new transaction')")
        assert scalar(target, "SELECT count(*) FROM promotion_sql_items") == 5
        assert scalar(target, "SELECT label FROM promotion_sql_items WHERE id=1") == "after"
        print(f"promotion-readers: sole snapshot-only reader preserves old view; wrap={wrap} PASS")
    finally:
        reader.close()
        pool.shutdown(wait=True)


base, root = Path(sys.argv[1]), Path(sys.argv[2])
source_port, target_port, target_pid = map(int, sys.argv[3:])
source, target = connect(source_port), connect(target_port)
readers = []
utility = None
try:
    query(source, "INSERT INTO promotion_sql_items VALUES (2,'warm')")
    wait_until(lambda: scalar(target, "SELECT count(*) FROM promotion_sql_items") == 2)
    query(target, "SELECT pg_wal_replay_pause()")
    if os.environ.get("PROMOTION_ONLY_SNAPSHOT"):
        only_snapshot()
        sys.exit(0)
    readers = [connect(target_port) for _ in range(4)]
    pids = [scalar(reader, "SELECT pg_backend_pid()") for reader in readers]
    before = [(1, "before"), (2, "warm")]
    for index, reader in enumerate(readers):
        query(reader, "BEGIN ISOLATION LEVEL REPEATABLE READ" if index in (0, 2) else "BEGIN")
        if index == 2:
            assert query(reader, "SELECT 1") == [(1,)]
        else:
            assert query(reader, "SELECT id,label FROM promotion_sql_items ORDER BY id") == before
    control(f'(load "{base}/t/promotion-readers-native.lisp")')
    crash_point = os.environ.get("PROMOTION_CRASH")
    crash_argument = f" :{crash_point}" if crash_point else ""
    control(f'(promotion-reader-test-install (list {" ".join(map(str, pids))}) #p"{root}/"{crash_argument})')
    query(source, "UPDATE promotion_sql_items SET label='after' WHERE id=1")
    query(source, "INSERT INTO promotion_sql_items VALUES (3,'new')")
    position = scalar(source, "SELECT pg_current_wal_flush_lsn()")
    wait_until(lambda: scalar(target, f"SELECT pg_last_wal_receive_lsn() >= '{position}'::pg_lsn"))
    utility = subprocess.Popen(
        [str(base / "bin/pg_ctl"), "promote", "-D", str(root / "copy"), "-w", "-t", "30"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    wait_until(lambda: (root / "enrolled").exists())
    assert int((root / "enrolled").read_text()) == target_pid
    readers[3].close()
    wait_until(lambda: (root / "eof-waiting").exists())
    assert int((root / "eof-waiting").read_text()) == target_pid
    (root / "release").touch()
    if crash_point:
        wait_until(lambda: (root / "crash").exists())
        assert int((root / "crash").read_text()) == target_pid
        os.kill(target_pid, signal.SIGKILL)
        output, _ = utility.communicate(timeout=30)
        assert utility.returncode != 0, output
        utility = None
        print(f"promotion-readers: retained reader crash at {crash_point} PASS")
        sys.exit(0)
    output, _ = utility.communicate(timeout=30)
    assert utility.returncode == 0, output
    utility = None
    assert scalar(target, "SELECT pg_is_in_recovery()") is False
    after = [(1, "after"), (2, "warm"), (3, "new")]
    for index in (0, 2):
        assert query(readers[index], "SELECT id,label FROM promotion_sql_items ORDER BY id") == before
    assert query(readers[1], "SELECT id,label FROM promotion_sql_items ORDER BY id") == after
    query(target, "INSERT INTO promotion_sql_items VALUES (4,'primary')")
    for index in (0, 2):
        assert query(readers[index], "SELECT id,label FROM promotion_sql_items ORDER BY id") == before
    assert scalar(readers[1], "SELECT count(*) FROM promotion_sql_items") == 4
    wait_until(lambda: ":GONE" in control("(if (promotion-reader-test-eof-gone-p) :gone :waiting)"))
    assert "ownership and EOF cleanup PASS" in control("(promotion-reader-test-verify)")
    for index, reader in enumerate(readers[:3]):
        assert scalar(reader, "SHOW transaction_read_only") == "on"
        if index == 0:
            try:
                query(reader, "INSERT INTO promotion_sql_items VALUES (999,'forbidden')")
            except psycopg2.Error as error:
                assert error.pgcode == "25006", error
            else:
                raise AssertionError("retained recovery transaction allowed a write")
            query(reader, "ROLLBACK")
        else:
            query(reader, "COMMIT")
        assert scalar(reader, "SHOW transaction_read_only") == "off"
        query(reader, f"INSERT INTO promotion_sql_items VALUES ({index + 5},'new transaction')")
    assert scalar(target, "SELECT count(*) FROM promotion_sql_items") == 7
    print("promotion-readers: RR/RC, snapshot-only old image, gated EOF and new writers PASS")
finally:
    (root / "release").touch()
    if utility is not None:
        utility.kill()
        utility.wait()
    for reader in readers:
        reader.close()
    source.close()
    target.close()
