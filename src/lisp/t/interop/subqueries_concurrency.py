"""Controlled EvalPlanQual comparisons for statement-owned subqueries."""
import subprocess
import re
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from subqueries import QueryConnection


def control(base, socket, expression):
    result = subprocess.run([os.environ.get("SBCL", "sbcl"), "--script", str(base / "t/control-client.lisp"),
                             "unix", str(socket)], input=expression + "\n(quit)\n",
                            text=True, capture_output=True, timeout=10, check=True)
    assert ";; error:" not in result.stdout + result.stderr, result.stdout + result.stderr
    return result.stdout


def wait_for_lock(admin, future, backend_pid, native, base, socket):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        assert not future.done(), ("mutation completed without waiting", future.result())
        if native:
            expression = (f'(let ((b (pg::lookup-backend {backend_pid}))) '
                          '(format t "SUBQUERY_WAIT=~a~%" '
                          '(if (and b (pg::backend-waiting-xid b)) 1 0)))')
            ready = "SUBQUERY_WAIT=1" in control(base, socket, expression)
        else:
            ready = admin.query(f"SELECT wait_event_type FROM pg_stat_activity WHERE pid={backend_pid}") == [["Lock"]]
        if ready:
            return
        time.sleep(0.02)
    raise AssertionError("mutation did not reach its conflicting writer")


def scenario(port, native, base, socket, kind):
    writer, mutation, admin = [QueryConnection(port, "subquery-recheck") for _ in range(3)]
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        for sql in ("DROP TABLE IF EXISTS subq_target", "DROP TABLE IF EXISTS subq_lookup",
                    "DROP SEQUENCE IF EXISTS subq_recheck_seq",
                    "CREATE TABLE subq_target(id int,k int,value int)",
                    "CREATE TABLE subq_lookup(k int,v int)",
                    "CREATE INDEX subq_target_id ON subq_target(id)",
                    "INSERT INTO subq_target VALUES(1,1,10)",
                    "INSERT INTO subq_lookup VALUES(1,100),(2,200)",
                    "CREATE SEQUENCE subq_recheck_seq"):
            admin.query(sql)
        writer.query("BEGIN")
        if kind == "deleted":
            writer.query("DELETE FROM subq_target WHERE id=1")
        else:
            key = 3 if kind in ("qualification", "delete-qualification") else 2
            writer.query(f"UPDATE subq_target SET k={key},value=20 WHERE id=1")
        mutation.query("BEGIN ISOLATION LEVEL REPEATABLE READ" if kind == "rr" else "BEGIN")
        if kind == "delete-qualification":
            sql = "DELETE FROM subq_target WHERE id=1 AND EXISTS(SELECT 1 FROM subq_lookup WHERE k=subq_target.k UNION ALL SELECT 1 WHERE false)"
        elif kind == "self":
            sql = "UPDATE subq_target SET value=(SELECT sum(i.value) FROM subq_target i WHERE i.id=subq_target.id) WHERE id=1"
        else:
            # UNION ALL prevents C's semijoin pullup, so this tests SubPlan EPQ.
            # The simple form is preserved below as a known optimizer boundary.
            body = "SELECT 1 FROM subq_lookup WHERE k=subq_target.k"
            if kind != "semijoin":
                body += " UNION ALL SELECT 1 WHERE false"
            sql = ("UPDATE subq_target SET value=(SELECT nextval('subq_recheck_seq')+v "
                   "FROM subq_lookup WHERE k=subq_target.k) WHERE id=1 AND "
                   f"EXISTS({body})")
        future = pool.submit(mutation.observe, sql)
        wait_for_lock(admin, future, mutation.backend_pid, native, base, socket)
        writer.query("UPDATE subq_lookup SET v=999 WHERE k=2")
        writer.query("ROLLBACK" if kind == "abort" else "COMMIT")
        outcome = future.result(timeout=20)
        mutation.query("ROLLBACK" if outcome[0] == "error" else "COMMIT")
        rows = admin.query("SELECT id,k,value FROM subq_target ORDER BY id")
        sequence = None if kind in ("self", "delete-qualification") else mutation.query("SELECT currval('subq_recheck_seq')")
        return outcome, rows, sequence
    finally:
        # Release a blocked mutation before joining its worker on failure.
        try:
            writer.query("ROLLBACK")
        except Exception:
            pass
        pool.shutdown(wait=True, cancel_futures=True)
        for connection in (writer, mutation, admin):
            connection.close()


def check(reference, native, base, socket):
    for kind in ("commit", "abort", "qualification", "delete-qualification", "deleted", "rr", "self"):
        expected = scenario(reference, False, base, socket, kind)
        actual = scenario(native, True, base, socket, kind)
        assert expected == actual, (kind, expected, actual)
        print(f"subqueries: concurrent {kind}: C differential PASS", flush=True)
    expected = scenario(reference, False, base, socket, "semijoin")
    actual = scenario(native, True, base, socket, "semijoin")
    assert expected == (("rows", [], [], ["UPDATE 0"]), [["1", "2", "20"]], [["1"]]), expected
    assert actual == (("rows", [], [], ["UPDATE 1"]), [["1", "2", "202"]], [["2"]]), actual
    print("subqueries: KNOWN DIVERGENCE: C semijoin EPQ versus retained Lisp SubPlan reproduced", flush=True)
    expected = resource_cases(reference, False, base, socket)
    actual = resource_cases(native, True, base, socket)
    assert expected == actual, ("spill/cancel", expected, actual)
    print("subqueries: nested spill/cancel: C differential PASS", flush=True)
    expected = ddl_case(reference, False, base, socket)
    actual = ddl_case(native, True, base, socket)
    assert expected == actual, ("index DDL", expected, actual)
    print("subqueries: indexed self-read and concurrent DDL: C differential PASS", flush=True)


def resource_cases(port, native, base, socket):
    connection, admin = [QueryConnection(port, "subquery-resources") for _ in range(2)]
    pool = ThreadPoolExecutor(max_workers=1)
    old_memory = None
    future = None
    try:
        admin.query("CREATE TABLE subq_big(id int)")
        admin.query("INSERT INTO subq_big VALUES " + ",".join(f"({i})" for i in range(300)))
        if native:
            output = control(base, socket,
                             '(format t "SUBQUERY_MEMORY=~d~%" pg::*query-work-memory-bytes*)')
            old_memory = int(re.search(r"SUBQUERY_MEMORY=(\d+)", output)[1])
            control(base, socket, '(setf pg::*query-work-memory-bytes* 8192)')
            spill_start = spill_counter(base, socket)
        sql = "SELECT ARRAY(SELECT id::text FROM subq_big ORDER BY id)"
        sorted_array = connection.observe(sql)
        assert sorted_array[0] == "rows", sorted_array
        if native:
            assert spill_counter(base, socket) > spill_start, "nested sort did not spill"
            assert_no_spill_files(base, socket)
            spill_start = spill_counter(base, socket)
        sql = ("SELECT ARRAY(SELECT a.id::text FROM subq_big a CROSS JOIN subq_big b "
               "CROSS JOIN subq_big c ORDER BY a.id)")
        future = pool.submit(connection.observe, sql)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert not future.done(), ("large child finished before cancellation", future.result())
            if native:
                ready = spill_counter(base, socket) > spill_start
            else:
                ready = admin.query(f"SELECT state FROM pg_stat_activity WHERE pid={connection.backend_pid}") == [["active"]]
            if ready:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("child did not start its resource work")
        assert admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})") == [["t"]]
        cancelled = future.result(timeout=20)
        assert cancelled == ("error", "57014"), cancelled
        after = connection.observe("SELECT (SELECT 7)")
        if native:
            assert_no_spill_files(base, socket)
        return sorted_array, cancelled, after
    finally:
        if future is not None and not future.done():
            admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})")
        pool.shutdown(wait=True, cancel_futures=True)
        if old_memory is not None:
            control(base, socket, f'(setf pg::*query-work-memory-bytes* {old_memory})')
        connection.close()
        admin.close()


def spill_counter(base, socket):
    output = control(base, socket,
                     '(format t "SUBQUERY_SPILLS=~d~%" pg::*query-temp-file-sequence*)')
    return int(re.search(r"SUBQUERY_SPILLS=(\d+)", output)[1])


def assert_no_spill_files(base, socket):
    output = control(base, socket,
                     '(format t "SUBQUERY_TEMP=~a~%" (namestring pg::*query-temp-root-directory*))')
    directory = Path(re.search(r"SUBQUERY_TEMP=([^\r\n]+)", output)[1])
    assert directory.is_relative_to(socket.parent / "native"), directory
    remaining = [str(path) for path in directory.rglob("*") if path.is_file()]
    assert not remaining, remaining


def ddl_case(port, native, base, socket):
    writer, mutation, ddl, admin = [QueryConnection(port, "subquery-ddl") for _ in range(4)]
    pool = ThreadPoolExecutor(max_workers=2)
    update = drop = None
    try:
        admin.query("TRUNCATE subq_target")
        admin.query("INSERT INTO subq_target VALUES(1,1,10)")
        writer.query("BEGIN")
        writer.query("UPDATE subq_target SET value=20 WHERE id=1")
        mutation.query("BEGIN")
        update = pool.submit(mutation.observe,
                             "UPDATE subq_target SET value=(SELECT i.value FROM subq_target i WHERE i.id=1) WHERE id=1")
        wait_for_lock(admin, update, mutation.backend_pid, native, base, socket)
        drop = pool.submit(ddl.observe, "DROP INDEX subq_target_id")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert not drop.done(), ("DDL ignored the registered writer", drop.result())
            if native:
                expression = ('(let ((node (pg::with-write-lock '
                              '(let ((pg::*backend* (pg::make-backend :database "postgres"))) '
                              '(pg::pg-class-relfilenode (pg::resolve-table-or-error "subq_target")))))) '
                              '(format t "SUBQUERY_DDL=~d~%" '
                              '(sb-thread:with-mutex (pg::*relation-lock-mutex*) '
                              '(if (gethash node pg::*relation-builders*) 1 0))))')
                ready = "SUBQUERY_DDL=1" in control(base, socket, expression)
            else:
                ready = admin.query(f"SELECT wait_event_type FROM pg_stat_activity WHERE pid={ddl.backend_pid}") == [["Lock"]]
            if ready:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("DDL did not reach its relation gate")
        writer.query("COMMIT")
        updated = update.result(timeout=20)
        assert not drop.done(), "DDL passed the still-open mutation transaction"
        mutation.query("COMMIT")
        dropped = drop.result(timeout=20)
        rows = admin.query("SELECT id,k,value FROM subq_target")
        return updated, dropped, rows
    finally:
        for connection in (writer, mutation):
            try:
                connection.query("ROLLBACK")
            except Exception:
                pass
        if drop is not None and not drop.done():
            admin.query(f"SELECT pg_cancel_backend({ddl.backend_pid})")
        pool.shutdown(wait=True, cancel_futures=True)
        for connection in (writer, mutation, ddl, admin):
            connection.close()


if __name__ == "__main__":
    check(int(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
