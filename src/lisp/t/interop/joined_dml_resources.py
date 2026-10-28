"""Source replay spilling, failed writes and cancellation release all resources."""
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dml_resources import RecordingConnection
from subqueries import QueryConnection
from subqueries_concurrency import control, spill_counter, assert_no_spill_files


def scenario(port, native, base, socket):
    connection = RecordingConnection(port, "joined-dml-resources")
    admin = QueryConnection(port, "joined-dml-admin")
    pool = ThreadPoolExecutor(max_workers=1)
    future = None
    old_memory = old_limit = None
    race_ready = socket.parent / "joined-index.ready"
    race_release = socket.parent / "joined-index.release"
    race_hook = False
    try:
        for sql in ("CREATE TABLE jdr_target(id int PRIMARY KEY,v int)",
                    "INSERT INTO jdr_target VALUES(1,10),(2,20)",
                    "CREATE TABLE jdr_source(k int,label varchar(126))"):
            admin.query(sql)
        admin.query("INSERT INTO jdr_source VALUES " + ",".join(
            f"({i},'{str(i).zfill(4)}{'x'*96}')" for i in range(1000)))
        admin.query("CREATE INDEX jdr_source_k ON jdr_source(k)")
        admin.query("ANALYZE jdr_source")
        if native:
            output = control(base, socket, '(format t "JD_MEMORY=~d~%JD_LIMIT=~d~%" pg::*query-work-memory-bytes* (or pg::*query-temp-file-limit-bytes* -1))')
            old_memory = int(re.search(r"JD_MEMORY=(\d+)", output)[1])
            old_limit = int(re.search(r"JD_LIMIT=(-?\d+)", output)[1])
            control(base, socket, '(setf pg::*query-work-memory-bytes* 8192)')
            before = spill_counter(base, socket)
        outcomes = []
        outcomes.append(connection.observe("UPDATE jdr_target t SET v=t.v+1 FROM jdr_source s WHERE t.id=s.k RETURNING t.id,t.v,s.label"))
        if native:
            assert spill_counter(base, socket) > before
            assert_no_spill_files(base, socket)
        before_rows, before_headers = connection.data_rows, connection.descriptions
        late = connection.observe("UPDATE jdr_target t SET v=1/(2-t.id) FROM jdr_source s WHERE t.id=s.k RETURNING t.id")
        assert late == ("error", "22012"), late
        assert (connection.data_rows, connection.descriptions) == (before_rows, before_headers)
        outcomes.extend((late, connection.observe("SELECT id,v FROM jdr_target ORDER BY id")))
        # An indexed, independent self-source must not retain B-tree leases across
        # the target mutation or its RETURNING expression subplan.
        admin.query("CREATE TABLE jdr_self(id int PRIMARY KEY,v int)")
        admin.query("INSERT INTO jdr_self VALUES " + ",".join(f"({i},{i})" for i in range(1000)))
        admin.query("ANALYZE jdr_self")
        if native:
            plan = admin.query("EXPLAIN SELECT id,v FROM jdr_self WHERE id=0")
            assert any("Index Scan" in row[0] for row in plan), plan
        outcomes.append(connection.observe("UPDATE jdr_self t SET v=s.v+1 FROM (SELECT id,v FROM jdr_self WHERE id=0) s WHERE t.id=s.id RETURNING t.id,t.v,(SELECT s.v)"))
        admin.query("CREATE TABLE jdr_keys(k int)")
        admin.query("INSERT INTO jdr_keys VALUES(0),(1),(2000)")
        admin.query("ANALYZE jdr_keys")
        if native:
            plan = admin.query("EXPLAIN SELECT u.k,s.v FROM jdr_keys u LEFT JOIN jdr_self s ON s.id=u.k")
            assert any("Index Scan" in row[0] and "Left Join" in row[0] for row in plan), plan
        outcomes.append(connection.observe("SELECT u.k,s.v FROM jdr_keys u LEFT JOIN jdr_self s ON s.id=u.k ORDER BY u.k"))
        outcomes.append(connection.observe("UPDATE jdr_target t SET v=s.v FROM jdr_keys u LEFT JOIN jdr_self s ON s.id=u.k WHERE t.id=CASE WHEN u.k=2000 THEN 2 ELSE u.k END RETURNING t.id,t.v,u.k,s.v"))
        outcomes.append(connection.observe("UPDATE jdr_target t SET v=s.v FROM (SELECT id,v FROM jdr_self WHERE id=0 LIMIT 1) s WHERE t.id=1 RETURNING t.id,s.v"))
        admin.query("CREATE TABLE jdr_race(id int,v int)")
        admin.query("INSERT INTO jdr_race VALUES " + ",".join(f"({i},{i})" for i in range(1000)))
        admin.query("CREATE INDEX jdr_race_idx ON jdr_race(id)")
        admin.query("ANALYZE jdr_race")
        race_sql = "UPDATE jdr_target t SET v=s.v FROM (SELECT id,v FROM jdr_race WHERE id=0) s WHERE t.id=1 RETURNING t.id,s.v"
        if native:
            control(base, socket, f'(load "{base}/t/interop/joined-dml-native.lisp")')
            race_ready.unlink(missing_ok=True)
            race_release.unlink(missing_ok=True)
            control(base, socket, f'(pg::install-joined-dml-index-race "{race_ready}" "{race_release}")')
            race_hook = True
            future = pool.submit(connection.observe, race_sql)
            deadline = time.monotonic() + 20
            while not (race_ready.exists() and race_ready.read_text()):
                assert not future.done(), future.result()
                assert time.monotonic() < deadline, "index race did not reach prepared plan"
                time.sleep(0.02)
            assert race_ready.read_text() == "T"
            admin.query("DROP INDEX jdr_race_idx")
            race_release.touch()
            outcome = future.result(timeout=20)
            control(base, socket, '(pg::remove-joined-dml-index-race)')
            race_hook = False
        else:
            admin.query("DROP INDEX jdr_race_idx")
            outcome = connection.observe(race_sql)
        assert outcome[0] == "rows" and outcome[2] == [["1", "0"]], outcome
        outcomes.append(outcome)
        if native:
            control(base, socket, '(setf pg::*query-temp-file-limit-bytes* 16384)')
        else:
            connection.query("SET work_mem='64kB'")
            connection.query("SET temp_file_limit=16")
        quota = connection.observe("DELETE FROM jdr_target t USING (SELECT k,label FROM jdr_source ORDER BY label) s WHERE t.id=s.k RETURNING t.id")
        assert quota == ("error", "53400"), quota
        outcomes.extend((quota, connection.observe("SELECT id,v FROM jdr_target ORDER BY id")))
        if native:
            assert_no_spill_files(base, socket)
            # This unordered source specifically overflows the new replay store.
            quota = connection.observe("UPDATE jdr_target t SET v=99 FROM jdr_source s WHERE t.id=s.k")
            assert quota == ("error", "53400"), quota
            assert_no_spill_files(base, socket)
            control(base, socket, '(setf pg::*query-temp-file-limit-bytes* nil)')
            before = spill_counter(base, socket)
        else:
            connection.query("SET temp_file_limit=-1")
        future = pool.submit(connection.observe,
            "UPDATE jdr_target t SET v=s.k FROM jdr_source s CROSS JOIN jdr_source u CROSS JOIN jdr_source w WHERE t.id=s.k RETURNING t.id")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert not future.done(), future.result()
            ready = (spill_counter(base, socket) > before if native else
                     admin.query(f"SELECT state FROM pg_stat_activity WHERE pid={connection.backend_pid}") == [["active"]])
            if ready:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("joined mutation did not start")
        assert admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})") == [["t"]]
        cancelled = future.result(timeout=20)
        assert cancelled == ("error", "57014"), cancelled
        outcomes.extend((cancelled, connection.observe("SELECT id,v FROM jdr_target ORDER BY id")))
        if native:
            assert_no_spill_files(base, socket)
        outcomes.append(connection.observe("SELECT 42"))
        return outcomes
    finally:
        race_release.touch()
        if future is not None and not future.done():
            admin.query(f"SELECT pg_cancel_backend({connection.backend_pid})")
        pool.shutdown(wait=True, cancel_futures=True)
        if race_hook:
            control(base, socket, '(pg::remove-joined-dml-index-race)')
        if old_memory is not None:
            control(base, socket, f'(setf pg::*query-work-memory-bytes* {old_memory} pg::*query-temp-file-limit-bytes* {"nil" if old_limit == -1 else old_limit})')
        connection.close()
        admin.close()


def check(reference, native, base, socket):
    outcomes = [scenario(port, native_p, base, socket)
                for port, native_p in ((reference, False), (native, True))]
    assert outcomes[0] == outcomes[1], outcomes
    print("joined-dml: replay spill, late error, quota, cancel, rollback and recovery PASS", flush=True)
