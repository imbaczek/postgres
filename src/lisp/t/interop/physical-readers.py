import subprocess
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg2


def connect(port, *, autocommit=True):
    connection = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                  dbname="postgres", connect_timeout=3)
    connection.autocommit = autocommit
    return connection


def query(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall() if cursor.description else None


def scalar(connection, sql):
    return query(connection, sql)[0][0]


def wait(connection, sql, expected):
    deadline = time.monotonic() + 20
    while scalar(connection, sql) != expected:
        assert time.monotonic() < deadline, (sql, expected)
        time.sleep(0.05)


def control(base, root, expression, *, server="target"):
    result = subprocess.run(["sbcl", "--script", str(base / "t/control-client.lisp"),
                             "unix", str(root / f"{server}.sock")],
                            input=expression + "\n(quit)\n", text=True,
                            capture_output=True, timeout=30, check=True)
    assert ";; error:" not in result.stdout, result.stdout + result.stderr
    return result.stdout


def begin_readers(readers, expected):
    for reader in readers:
        assert query(reader, "SELECT id,label FROM promotion_sql_items ORDER BY id") == expected


def received(source, target):
    position = scalar(source, "SELECT pg_current_wal_flush_lsn()")
    wait(target, f"SELECT pg_last_wal_receive_lsn() >= '{position}'::pg_lsn", True)
    return position


def applied(source, target):
    position = received(source, target)
    wait(target, f"SELECT pg_last_wal_replay_lsn() >= '{position}'::pg_lsn", True)
    return position


def running_snapshot_end(base, root):
    output = control(base, root, """
      (sb-thread:with-mutex (*wal-lock*)
        (let ((end nil))
          (wal-scan (lambda (record)
                      (when (= (wal-record-type record) +wal-record-running-xacts+)
                        (setf end (wal-record-end record)))))
          (format t "~&RUNNING-END=~d~%" end)))
    """, server="source")
    return int(re.search(r"RUNNING-END=(\d+)", output)[1])


def replay_at(target, position):
    lsn = f"{position >> 32:X}/{position & 0xffffffff:X}"
    wait(target, f"SELECT pg_last_wal_replay_lsn() = '{lsn}'::pg_lsn", True)


def check(base, root, source, target, readers, older):
    before = [(1, "before")]
    begin_readers(readers, before)
    control(base, root, f'(load "{base}/t/physical-row-replay-native.lisp")')
    control(base, root, "(row-replay-test-install)")
    query(source, "UPDATE promotion_sql_items SET label='after' WHERE id=1")
    query(source, "INSERT INTO promotion_sql_items VALUES (2,'new')")
    applied(source, target)
    assert query(target, "SELECT id,label FROM promotion_sql_items ORDER BY id") == [(1, "after"), (2, "new")]
    begin_readers(readers, before)
    assert "preserved PASS" in control(base, root, "(row-replay-test-verify)")
    for reader in readers:
        reader.commit()
    print("physical-readers: safe commits advance with two retained RR views PASS", flush=True)

    query(older, "UPDATE promotion_sql_items SET label='older-pending' WHERE id=1")
    query(source, "INSERT INTO row_barriers VALUES (1)")
    applied(source, target)
    current = [(1, "after"), (2, "new")]
    begin_readers(readers, current)
    older.commit()
    anchor = running_snapshot_end(base, root)
    query(source, "INSERT INTO row_barriers VALUES (2)")
    received(source, target)
    time.sleep(0.3)
    replay_at(target, anchor)
    assert scalar(target, "SELECT count(*) FROM row_barriers") == 1
    begin_readers(readers, current)
    readers[0].commit()
    time.sleep(0.2)
    replay_at(target, anchor)
    readers[1].commit()
    applied(source, target)
    assert scalar(target, "SELECT count(*) FROM row_barriers") == 2
    print("physical-readers: older writer defers without skipping a later safe batch PASS", flush=True)

    current = [(1, "older-pending"), (2, "new")]
    query(older, "UPDATE promotion_sql_items SET label='mixed-first' WHERE id=1")
    query(source, "INSERT INTO row_barriers VALUES (50)")
    applied(source, target)
    begin_readers(readers, current)
    query(older, "UPDATE promotion_sql_items SET label='older-pending' WHERE id=1")
    query(source, "INSERT INTO row_barriers VALUES (51)")
    anchor = running_snapshot_end(base, root)
    received(source, target)
    control(base, root, "(physical-recovery-replay)")
    replay_at(target, anchor)
    assert scalar(target, "SELECT count(*) FROM row_barriers WHERE id=51") == 0
    begin_readers(readers, current)
    older.commit()
    for reader in readers:
        reader.commit()
    applied(source, target)
    query(source, "DELETE FROM row_barriers WHERE id>=50")
    applied(source, target)
    print("physical-readers: an older page contributor blocks a newer commit PASS", flush=True)

    begin_readers(readers, current)
    control(base, root, f'(load "{base}/t/physical-row-replay-native.lisp")', server="source")
    control(base, root, "(row-replay-test-legacy-next-page-batch)", server="source")
    query(source, "INSERT INTO row_barriers VALUES (52)")
    anchor = running_snapshot_end(base, root)
    query(source, "INSERT INTO row_barriers VALUES (53)")
    received(source, target)
    control(base, root, "(physical-recovery-replay)")
    replay_at(target, anchor)
    assert scalar(target, "SELECT count(*) FROM row_barriers WHERE id>=50") == 0
    begin_readers(readers, current)
    for reader in readers:
        reader.commit()
    applied(source, target)
    assert scalar(target, "SELECT count(*) FROM row_barriers WHERE id>=50") == 2
    query(source, "DELETE FROM row_barriers WHERE id>=50")
    applied(source, target)
    print("physical-readers: unflagged legacy WAL defers without blocking later recovery PASS", flush=True)

    begin_readers(readers, current)
    query(source, "INSERT INTO row_barriers VALUES (3)")
    row_end = scalar(source, "SELECT pg_current_wal_flush_lsn()")
    query(source, "CREATE TABLE row_ddl_barrier (id integer)")
    query(source, "INSERT INTO row_barriers VALUES (4)")
    received(source, target)
    wait(target, f"SELECT pg_last_wal_replay_lsn() >= '{row_end}'::pg_lsn", True)
    assert scalar(target, "SELECT pg_last_wal_replay_lsn()") == row_end
    assert scalar(target, "SELECT count(*) FROM row_barriers") == 3
    begin_readers(readers, current)
    for reader in readers:
        reader.commit()
    applied(source, target)
    assert scalar(target, "SELECT count(*) FROM row_barriers") == 4
    assert scalar(target, "SELECT count(*) FROM row_ddl_barrier") == 0
    print("physical-readers: safe prefix applies before a catalog barrier PASS", flush=True)

    begin_readers(readers, current)
    query(source, "UPDATE promotion_sql_items SET label='before-vacuum' WHERE id=1")
    row_end = scalar(source, "SELECT pg_current_wal_flush_lsn()")
    query(source, "VACUUM promotion_sql_items")
    received(source, target)
    wait(target, f"SELECT pg_last_wal_replay_lsn() >= '{row_end}'::pg_lsn", True)
    assert scalar(target, "SELECT pg_last_wal_replay_lsn()") == row_end
    begin_readers(readers, current)
    assert scalar(target, "SELECT label FROM promotion_sql_items WHERE id=1") == "before-vacuum"
    for reader in readers:
        reader.commit()
    applied(source, target)
    print("physical-readers: vacuum waits while old heap versions remain visible PASS", flush=True)

    current = [(1, "before-vacuum"), (2, "new")]
    begin_readers(readers, current)
    query(source, "DELETE FROM promotion_sql_items WHERE id=1")
    delete_end = applied(source, target)
    begin_readers(readers, current)
    marker, release = root / "vacuum-staged", root / "vacuum-release"
    control(base, root, f'(load "{base}/t/physical-row-replay-native.lisp")', server="source")
    control(base, root, f'(row-replay-test-pause-vacuum "{marker}" "{release}")', server="source")

    def vacuum():
        connection = connect(source.info.port)
        try:
            query(connection, "VACUUM promotion_sql_items")
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(vacuum)
        try:
            deadline = time.monotonic() + 10
            while not marker.exists():
                if future.done():
                    future.result()
                    raise AssertionError("vacuum did not reach its staged-page barrier")
                assert time.monotonic() < deadline
                time.sleep(0.05)
            query(source, "INSERT INTO promotion_sql_items VALUES (3,'vacuum-interleaving')")
            anchor = running_snapshot_end(base, root)
            received(source, target)
            control(base, root, "(physical-recovery-replay)")
            begin_readers(readers, current)
            replay_at(target, anchor)
            assert scalar(target, "SELECT count(*) FROM promotion_sql_items WHERE id=3") == 0
        finally:
            release.touch()
            future.result(timeout=10)
    for reader in readers:
        reader.commit()
    applied(source, target)
    assert query(target, "SELECT id,label FROM promotion_sql_items ORDER BY id") == [
        (2, "new"), (3, "vacuum-interleaving")]
    print("physical-readers: maintenance hidden in a row commit cannot discard an old view PASS", flush=True)


def main():
    base, root = Path(sys.argv[1]), Path(sys.argv[2])
    source = connect(int(sys.argv[3]))
    target = connect(int(sys.argv[4]))
    older = connect(int(sys.argv[3]), autocommit=False)
    readers = [connect(int(sys.argv[4]), autocommit=False) for _ in range(2)]
    for reader in readers:
        reader.set_session(isolation_level="REPEATABLE READ")
    try:
        if len(sys.argv) > 5:
            query(readers[0], "SELECT count(*) FROM promotion_sql_items")
            marker = root / "crash-marker"
            control(base, root, f'(load "{base}/t/physical-row-replay-native.lisp")')
            control(base, root, f'(row-replay-test-arm-crash :{sys.argv[5]} "{marker}")')
            (root / "reader-ready").touch()
            while not (root / "reader-release").exists():
                time.sleep(0.05)
        else:
            check(base, root, source, target, readers, older)
    finally:
        for connection in [source, target, older, *readers]:
            connection.close()


if __name__ == "__main__":
    main()
