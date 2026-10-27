"""Controlled INSERT conflict and DELETE RETURNING comparisons with C."""
from concurrent.futures import ThreadPoolExecutor

from subqueries import QueryConnection
from subqueries_concurrency import control, wait_for_lock


def reset(admin, rows):
    admin.query("DROP TABLE IF EXISTS dml_concurrent")
    admin.query("CREATE TABLE dml_concurrent(id int PRIMARY KEY,v int)")
    if rows:
        admin.query("INSERT INTO dml_concurrent VALUES " + ",".join(
            f"({key},{value})" for key, value in rows))


def finish(connection, outcome):
    connection.query("ROLLBACK" if outcome[0] == "error" else "COMMIT")


def rows(admin):
    return admin.query("SELECT id,v FROM dml_concurrent ORDER BY id")


def clean_registry(native, base, socket):
    if native:
        output = control(base, socket,
            '(sb-thread:with-mutex (pg::*on-conflict-row-lock-mutex*) '
            '(format t "DML_LOCKS=~d/~d~%" '
            '(hash-table-count pg::*on-conflict-row-locks*) '
            '(hash-table-count pg::*on-conflict-xid-row-locks*)))')
        assert "DML_LOCKS=0/0" in output, output


def cleanup(peers, admin, pool, pending):
    # Roll back an idle holder before joining a mutation that may still wait.
    busy = {id(connection) for future, connection in pending if not future.done()}
    for connection in peers:
        if id(connection) not in busy and connection is not admin:
            try:
                connection.query("ROLLBACK")
            except Exception:
                pass
    for future, connection in pending:
        if not future.done():
            try:
                admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})")
            except Exception:
                pass
    for future, connection in pending:
        try:
            future.result(timeout=20)
        except Exception:
            pass
        if future.done():
            try:
                connection.query("ROLLBACK")
            except Exception:
                pass
    pool.shutdown(wait=True, cancel_futures=True)
    for connection in peers:
        connection.close()


def insert_wait(port, native, base, socket, action, release, isolation):
    writer, mutation, admin = peers = [QueryConnection(port, "dml-insert-wait") for _ in range(3)]
    pool = ThreadPoolExecutor(max_workers=1)
    pending = []
    try:
        reset(admin, [])
        mutation.query("BEGIN ISOLATION LEVEL REPEATABLE READ" if isolation == "rr" else "BEGIN")
        mutation.query("SELECT count(*) FROM dml_concurrent")
        writer.query("BEGIN")
        writer.query("INSERT INTO dml_concurrent VALUES(1,10)")
        update = ("UPDATE SET v=dml_concurrent.v+excluded.v" +
                  (" WHERE false" if action == "update-false" else "")
                  if action.startswith("update") else "NOTHING")
        sql = f"INSERT INTO dml_concurrent VALUES(1,30) ON CONFLICT(id) DO {update} RETURNING id,v"
        future = pool.submit(mutation.observe, sql)
        pending.append((future, mutation))
        wait_for_lock(admin, future, mutation.backend_pid, native, base, socket)
        writer.query(release)
        outcome = future.result(timeout=20)
        if isolation == "rr" and release == "COMMIT":
            assert outcome == ("error", "40001"), outcome
            expected = [["1", "10"]]
        else:
            assert outcome[0] == "rows", outcome
            expected = [["1", "30" if release == "ROLLBACK" else "40" if action == "update" else "10"]]
        finish(mutation, outcome)
        actual = rows(admin)
        assert actual == expected, (action, release, isolation, actual)
        clean_registry(native, base, socket)
        return outcome, actual
    finally:
        cleanup(peers, admin, pool, pending)


def changed_conflict(port, native, base, socket, kind):
    writer, mutation, admin = peers = [QueryConnection(port, "dml-changed-conflict") for _ in range(3)]
    pool = ThreadPoolExecutor(max_workers=1)
    pending = []
    try:
        reset(admin, [(1, 10)])
        writer.query("BEGIN")
        writer.query({
            "key": "UPDATE dml_concurrent SET id=2,v=20 WHERE id=1",
            "value": "UPDATE dml_concurrent SET v=20 WHERE id=1",
            "delete": "DELETE FROM dml_concurrent WHERE id=1",
        }[kind])
        mutation.query("BEGIN")
        future = pool.submit(mutation.observe,
            "INSERT INTO dml_concurrent VALUES(1,30) ON CONFLICT(id) DO UPDATE SET v=dml_concurrent.v+excluded.v RETURNING id,v")
        pending.append((future, mutation))
        wait_for_lock(admin, future, mutation.backend_pid, native, base, socket)
        writer.query("COMMIT")
        outcome = future.result(timeout=20)
        assert outcome[0] == "rows", outcome
        finish(mutation, outcome)
        actual = rows(admin)
        expected = {"key": [["1", "30"], ["2", "20"]],
                    "value": [["1", "50"]], "delete": [["1", "30"]]}[kind]
        assert actual == expected, (kind, actual)
        clean_registry(native, base, socket)
        return outcome, actual
    finally:
        cleanup(peers, admin, pool, pending)


def false_where_lock(port, native, base, socket, operation, release):
    writer, mutation, admin = peers = [QueryConnection(port, "dml-false-where-lock") for _ in range(3)]
    pool = ThreadPoolExecutor(max_workers=1)
    pending = []
    try:
        reset(admin, [(1, 10)])
        writer.query("BEGIN")
        skipped = writer.observe(
            "INSERT INTO dml_concurrent VALUES(1,99) ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE false RETURNING id,v")
        assert skipped[0] == "rows" and skipped[2] == [] and skipped[3] == ["INSERT 0 0"], skipped
        mutation.query("BEGIN")
        sql = ("UPDATE dml_concurrent SET v=v+1 WHERE id=1 RETURNING id,v" if operation == "update" else
               "DELETE FROM dml_concurrent WHERE id=1 RETURNING id,v")
        future = pool.submit(mutation.observe, sql)
        pending.append((future, mutation))
        wait_for_lock(admin, future, mutation.backend_pid, native, base, socket)
        writer.query(release)
        outcome = future.result(timeout=20)
        assert outcome[0] == "rows", outcome
        assert outcome[2] == ([["1", "11"]] if operation == "update" else [["1", "10"]]), outcome
        finish(mutation, outcome)
        actual = rows(admin)
        assert actual == ([["1", "11"]] if operation == "update" else []), actual
        clean_registry(native, base, socket)
        return skipped, outcome, actual
    finally:
        cleanup(peers, admin, pool, pending)


def delete_recheck(port, native, base, socket, kind):
    writer, mutation, admin = peers = [QueryConnection(port, "dml-delete-recheck") for _ in range(3)]
    pool = ThreadPoolExecutor(max_workers=1)
    pending = []
    try:
        reset(admin, [(1, 10)])
        mutation.query("BEGIN ISOLATION LEVEL REPEATABLE READ" if kind == "rr" else "BEGIN")
        mutation.query("SELECT count(*) FROM dml_concurrent")
        writer.query("BEGIN")
        writer.query("UPDATE dml_concurrent SET v=20 WHERE id=1")
        condition = "id=1 AND v=10" if kind == "skip" else "id=1"
        future = pool.submit(mutation.observe,
                             f"DELETE FROM dml_concurrent WHERE {condition} RETURNING id,v")
        pending.append((future, mutation))
        wait_for_lock(admin, future, mutation.backend_pid, native, base, socket)
        writer.query("COMMIT")
        outcome = future.result(timeout=20)
        if kind == "rr":
            assert outcome == ("error", "40001"), outcome
        else:
            assert outcome[0] == "rows", outcome
            assert outcome[2] == ([] if kind == "skip" else [["1", "20"]]), outcome
        finish(mutation, outcome)
        actual = rows(admin)
        assert actual == ([] if kind == "new-version" else [["1", "20"]]), actual
        clean_registry(native, base, socket)
        return outcome, actual
    finally:
        cleanup(peers, admin, pool, pending)


def cancel_row_lock(port, native, base, socket):
    writer, mutation, admin = peers = [QueryConnection(port, "dml-row-lock-cancel") for _ in range(3)]
    pool = ThreadPoolExecutor(max_workers=1)
    pending = []
    try:
        reset(admin, [(1, 10)])
        writer.query("BEGIN")
        writer.query("INSERT INTO dml_concurrent VALUES(1,99) ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE false")
        mutation.query("BEGIN")
        future = pool.submit(mutation.observe,
            "INSERT INTO dml_concurrent VALUES(1,30) ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING id,v")
        pending.append((future, mutation))
        wait_for_lock(admin, future, mutation.backend_pid, native, base, socket)
        assert admin.query(f"SELECT pg_cancel_backend({mutation.backend_pid})") == [["t"]]
        cancelled = future.result(timeout=20)
        assert cancelled == ("error", "57014"), cancelled
        mutation.query("ROLLBACK")
        recovered = mutation.observe("SELECT 42")
        assert recovered[2] == [["42"]], recovered
        writer.query("ROLLBACK")
        deleted = mutation.observe("DELETE FROM dml_concurrent RETURNING id,v")
        assert deleted[0] == "rows" and deleted[2] == [["1", "10"]], deleted
        clean_registry(native, base, socket)
        return cancelled, recovered, deleted, rows(admin)
    finally:
        cleanup(peers, admin, pool, pending)


def opposite_lock_timeout(port, native, base, socket):
    first, second, admin = peers = [QueryConnection(port, "dml-opposite-lock") for _ in range(3)]
    pool = ThreadPoolExecutor(max_workers=2)
    pending = []
    try:
        reset(admin, [(1, 10), (2, 20)])
        for connection, key in ((first, 1), (second, 2)):
            connection.query("BEGIN")
            connection.query(f"INSERT INTO dml_concurrent VALUES({key},99) ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE false")
        first.query("SET statement_timeout=2000")
        second.query("SET statement_timeout=200")
        left = pool.submit(first.observe,
            "INSERT INTO dml_concurrent VALUES(2,99) ON CONFLICT(id) DO UPDATE SET v=dml_concurrent.v+1 RETURNING id,v")
        pending.append((left, first))
        wait_for_lock(admin, left, first.backend_pid, native, base, socket)
        right = pool.submit(second.observe,
            "INSERT INTO dml_concurrent VALUES(1,99) ON CONFLICT(id) DO UPDATE SET v=dml_concurrent.v+1 RETURNING id,v")
        pending.append((right, second))
        timed_out = right.result(timeout=20)
        assert timed_out == ("error", "57014"), timed_out
        second.query("ROLLBACK")
        unblocked = left.result(timeout=20)
        assert unblocked[0] == "rows" and unblocked[2] == [["2", "21"]], unblocked
        first.query("COMMIT")
        actual = rows(admin)
        assert actual == [["1", "10"], ["2", "21"]], actual
        clean_registry(native, base, socket)
        return timed_out, unblocked, actual
    finally:
        cleanup(peers, admin, pool, pending)


def check(reference, native, base, socket):
    cases = []
    for isolation in ("rc", "rr"):
        for release in ("COMMIT", "ROLLBACK"):
            for action in ("nothing", "update", "update-false"):
                cases.append((f"insert-{action}-{release.lower()}-{isolation}", insert_wait,
                              (action, release, isolation)))
    cases += [(f"changed-{kind}", changed_conflict, (kind,)) for kind in ("key", "value", "delete")]
    cases += [(f"false-where-{operation}-{release.lower()}", false_where_lock, (operation, release))
              for operation in ("update", "delete") for release in ("COMMIT", "ROLLBACK")]
    cases += [(f"delete-{kind}", delete_recheck, (kind,)) for kind in ("new-version", "skip", "rr")]
    cases += [("row-lock-cancel", cancel_row_lock, ()),
              ("opposite-lock-timeout", opposite_lock_timeout, ())]
    for name, scenario, arguments in cases:
        expected = scenario(reference, False, base, socket, *arguments)
        actual = scenario(native, True, base, socket, *arguments)
        assert expected == actual, (name, expected, actual)
        print(f"dml: concurrency {name}: C differential PASS", flush=True)
    print(f"dml: {len(cases)} controlled concurrency cases PASS", flush=True)
