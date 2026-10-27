"""Spilling, buffered RETURNING, late errors, cancellation and DML rollback."""
import re
import time
from concurrent.futures import ThreadPoolExecutor
from subqueries import QueryConnection
from subqueries_concurrency import control, spill_counter, assert_no_spill_files


class RecordingConnection(QueryConnection):
    def __init__(self, *args):
        self.data_rows = 0
        self.descriptions = 0
        super().__init__(*args)

    def receive(self):
        kind, body = super().receive()
        if kind == b"D":
            self.data_rows += 1
        if kind == b"T":
            self.descriptions += 1
        return kind, body


def scenario(port, native, base, socket):
    connection = RecordingConnection(port, "dml-resources")
    admin = QueryConnection(port, "dml-resource-admin")
    pool = ThreadPoolExecutor(max_workers=1)
    future = None
    old_memory = old_limit = None
    try:
        for sql in ("CREATE TABLE dml_big(id int,label text)",
                    "CREATE TABLE dml_sink(id int PRIMARY KEY,label text)",
                    "CREATE TABLE dml_cancel(id int,label text)"):
            admin.query(sql)
        admin.query("INSERT INTO dml_big VALUES " + ",".join(
            f"({i},'{str(i).zfill(4)}{'x'*96}')" for i in range(1000)))
        admin.query("CREATE INDEX dml_big_id ON dml_big(id)")
        admin.query("ANALYZE dml_big")
        if native:
            output = control(base, socket, '(format t "DML_MEMORY=~d~%DML_LIMIT=~d~%" pg::*query-work-memory-bytes* (or pg::*query-temp-file-limit-bytes* -1))')
            old_memory = int(re.search(r"DML_MEMORY=(\d+)", output)[1])
            old_limit = int(re.search(r"DML_LIMIT=(-?\d+)", output)[1])
            control(base, socket, '(setf pg::*query-work-memory-bytes* 8192)')
            before = spill_counter(base, socket)
        outcomes = []
        for sql in (
            "INSERT INTO dml_sink SELECT id,label FROM dml_big ORDER BY id DESC RETURNING id",
            "INSERT INTO dml_sink WITH q AS MATERIALIZED(SELECT id,label FROM dml_big) SELECT id+2000,label FROM q RETURNING id,(SELECT count(*) FROM dml_sink)",
            "INSERT INTO dml_sink SELECT id+1000,label FROM dml_big WHERE id=500 RETURNING id",
            "INSERT INTO dml_sink SELECT id+4000,label FROM dml_sink WHERE id=500 RETURNING id",
        ):
            outcome = connection.observe(sql)
            assert outcome[0] == "rows", (sql, outcome)
            outcomes.append(outcome)
        before_rows = connection.data_rows
        before_descriptions = connection.descriptions
        late = connection.observe(
            "INSERT INTO dml_cancel SELECT id,CASE WHEN id=999 THEN (1/(999-id))::text ELSE label END FROM dml_big RETURNING id,label")
        assert late == ("error", "22012"), late
        assert connection.data_rows == before_rows, (native, connection.data_rows - before_rows)
        assert connection.descriptions == before_descriptions, (native, connection.descriptions - before_descriptions)
        assert not connection.commands, connection.commands
        outcomes.extend((late, connection.observe("SELECT count(*) FROM dml_cancel")))
        if native:
            assert spill_counter(base, socket) > before, "DML source/CTE did not spill"
            assert_no_spill_files(base, socket)
            before = spill_counter(base, socket)
        future = pool.submit(connection.observe,
            "INSERT INTO dml_cancel SELECT a.id,a.label FROM dml_big a CROSS JOIN dml_big b CROSS JOIN dml_big c")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert not future.done(), future.result()
            ready = (spill_counter(base, socket) > before if native else
                     admin.query(f"SELECT state FROM pg_stat_activity WHERE pid={connection.backend_pid}") == [["active"]])
            if ready:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("INSERT SELECT did not start")
        assert admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})") == [["t"]]
        cancelled = future.result(timeout=20)
        assert cancelled == ("error", "57014"), cancelled
        outcomes.extend((cancelled, connection.observe("SELECT count(*) FROM dml_cancel")))
        if native:
            assert_no_spill_files(base, socket)
            control(base, socket, '(setf pg::*query-temp-file-limit-bytes* 16384)')
        else:
            connection.query("SET work_mem='64kB'")
            connection.query("SET temp_file_limit=16")
        quota = connection.observe(
            "INSERT INTO dml_cancel SELECT id,label FROM dml_big ORDER BY label")
        assert quota == ("error", "53400"), quota
        outcomes.extend((quota, connection.observe("SELECT count(*) FROM dml_cancel")))
        before_rows = connection.data_rows
        quota = connection.observe("INSERT INTO dml_cancel SELECT id,label FROM dml_big RETURNING id,label")
        assert quota == ("error", "53400"), quota
        assert connection.data_rows == before_rows
        outcomes.extend((quota, connection.observe("SELECT count(*) FROM dml_cancel")))
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
        connection.close()
        admin.close()


def check(reference, native, base, socket):
    outcomes = [scenario(port, native_p, base, socket)
                for port, native_p in ((reference, False), (native, True))]
    assert outcomes[0] == outcomes[1], outcomes
    print("dml: spill, buffered RETURNING, late error, cancel, quota, rollback and recovery PASS", flush=True)
