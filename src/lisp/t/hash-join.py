"""Exercise the SQL hash join path with in-memory and spilled builds."""

import argparse
import contextlib
import datetime as dt
import json
import random
import runpy
import shutil
import socket
import struct
import time
from pathlib import Path


fixture = runpy.run_path(str(Path(__file__).with_name("command-complete.py")))
from parking_wire import WireError


class CancelConnection(fixture["Connection"]):
    def receive(self):
        kind, body = super().receive()
        if kind == b"K":
            self.cancel_key = struct.unpack("!II", body)
        return kind, body


def verify_join(connection):
    for table, key in (("hash_left", "a"), ("hash_right", "b")):
        payload = ", payload text" if table == "hash_right" else ""
        connection.query(f"CREATE TABLE {table} (id integer, {key} integer{payload})")
        for start in range(0, 1200, 200):
            rows = ",".join(f"({row},{row % 200})" for row in range(start, start + 200))
            connection.query(f"INSERT INTO {table} (id, {key}) VALUES {rows}")
        connection.query(f"INSERT INTO {table} (id, {key}) VALUES (1200, NULL)")
        connection.query(f"ANALYZE {table}")

    query = (
        "SELECT count(*) FROM hash_left l JOIN hash_right r "
        "ON l.a = r.b AND l.id < r.id"
    )
    assert "Hash Inner Join" in connection.query("EXPLAIN " + query)[0][0]
    assert connection.query(query) == [["3000"]]
    analyzed = connection.query("EXPLAIN ANALYZE " + query)[0][0]
    assert "Rows Removed by Join Filter: 4200" in analyzed, analyzed
    return query


def bind_join_portal(connection):
    query = b"SELECT r.id FROM hash_left l JOIN hash_right r ON l.a = r.b WHERE l.id = 7"
    connection.send(b"P", b"hash_stmt\0" + query + bytes(3))
    connection.send(b"B", b"hash_portal\0hash_stmt\0" + bytes(6))


def pull_join_portal(connection, rows):
    connection.send(b"E", b"hash_portal\0" + struct.pack("!I", 1))
    connection.send(b"H", b"")
    while True:
        kind, body = connection.receive()
        assert kind != b"E", body
        if kind == b"D":
            rows.append(int(body[6:]))
        if kind in (b"s", b"C"):
            return kind


def verify_suspended_join(server, connection):
    bind_join_portal(connection)
    rows = []
    assert pull_join_portal(connection, rows) == b"s"
    assert spill_files(server), "the suspended join never spilled"
    expected = list(range(7, 1200, 200))
    updated, deleted = [row for row in expected if row not in rows][:2]
    with contextlib.closing(fixture["Connection"](server.port)) as writer:
        writer.query("SET statement_timeout = '250ms'")
        try:
            writer.query("TRUNCATE hash_right")
        except WireError as error:
            assert error.sqlstate == "57014", error
        else:
            raise AssertionError("TRUNCATE replaced storage during portal suspension")
        writer.query("RESET statement_timeout")
        writer.query(f"UPDATE hash_right SET b = -1 WHERE id = {updated}")
        writer.query(f"DELETE FROM hash_right WHERE id = {deleted}")
        while pull_join_portal(connection, rows) == b"s":
            pass
        connection.send(b"C", b"Shash_stmt\0")
        connection.send(b"S", b"")
        assert connection.receive()[0] == b"3"
        assert connection.receive()[0] == b"Z"
        assert sorted(rows) == expected
        assert writer.query("SELECT id FROM hash_right WHERE b = 7 ORDER BY id") == [
            [str(row)] for row in expected if row not in (updated, deleted)
        ]
        writer.query(f"UPDATE hash_right SET b = 7 WHERE id = {updated}")
        writer.query(f"INSERT INTO hash_right (id, b) VALUES ({deleted}, 7)")
        writer.query("BEGIN")
        writer.query("TRUNCATE hash_right")
        assert writer.query("SELECT count(*) FROM hash_right") == [["0"]]
        writer.query("ROLLBACK")


def spill_files(server):
    return set((server.data / "pgsql_tmp").rglob("pg-query-*.tmp"))


def verify_early_close(server, connection):
    before = spill_files(server)
    bind_join_portal(connection)
    assert pull_join_portal(connection, []) == b"s"
    assert spill_files(server) - before, "the suspended join never spilled"
    connection.send(b"C", b"Phash_portal\0")
    connection.send(b"S", b"")
    assert connection.receive()[0] == b"3"
    assert connection.receive()[0] == b"Z"
    assert spill_files(server) == before
    with contextlib.closing(fixture["Connection"](server.port)) as observer:
        observer.query("BEGIN")
        observer.query("CREATE INDEX hash_released ON hash_right(b)")
        observer.query("ROLLBACK")


def verify_crash_cleanup(server, query):
    committed = (
        "SELECT id, a FROM hash_left ORDER BY id",
        "SELECT id, b, payload FROM hash_right ORDER BY id",
    )
    with contextlib.closing(fixture["Connection"](server.port)) as connection:
        expected = [connection.query(statement) for statement in committed]
        connection.query("SET work_mem = '64kB'")
        bind_join_portal(connection)
        assert pull_join_portal(connection, []) == b"s"
        files = spill_files(server)
        assert files, "the suspended join never spilled"
        server.stop(immediate=True)
    assert all(path.exists() for path in files), "the crash did not leave spill files"
    server.start()
    assert not spill_files(server)
    with contextlib.closing(fixture["Connection"](server.port)) as connection:
        assert [connection.query(statement) for statement in committed] == expected
        assert connection.query(query) == [["3000"]]


def verify_disconnect_cleanup(server):
    before = spill_files(server)
    with contextlib.closing(fixture["Connection"](server.port)) as connection:
        connection.query("SET work_mem = '64kB'")
        bind_join_portal(connection)
        assert pull_join_portal(connection, []) == b"s"
        assert spill_files(server) - before
    deadline = time.monotonic() + 5
    while spill_files(server) != before:
        if time.monotonic() >= deadline:
            raise AssertionError("disconnected join still owns temporary files")
        time.sleep(0.003)


def verify_spill_quota_cleanup(server, connection, query):
    before = spill_files(server)
    server.control("(setf pg::*query-temp-file-limit-bytes* 8192)")
    try:
        try:
            connection.query(query)
        except WireError as error:
            assert error.sqlstate == "53400", error
        else:
            raise AssertionError("hash join exceeded the temporary-file quota")
        assert spill_files(server) == before
    finally:
        server.control("(setf pg::*query-temp-file-limit-bytes* nil)")
    assert connection.query(query) == [["3000"]]


def cancel_request(connection):
    with socket.create_connection(connection.socket.getpeername(), timeout=5) as cancel:
        cancel.sendall(struct.pack("!IIII", 16, 80877102, *connection.cancel_key))
        assert cancel.recv(1) == b""
    assert failed_query(connection).sqlstate == "57014"


def failed_query(connection):
    failure = None
    while True:
        kind, body = connection.receive()
        if kind == b"E":
            failure = WireError(connection.error_fields(body))
        elif kind == b"Z":
            break
    assert failure is not None
    return failure


def verify_cancel_cleanup(server, query):
    before = spill_files(server)
    with contextlib.closing(CancelConnection(server.port)) as connection:
        connection.query("SET work_mem = '64kB'")
        connection.query("BEGIN")
        connection.query("UPDATE hash_left SET a = 1")
        connection.query("UPDATE hash_right SET b = 1")
        assert "Hash Inner Join" in connection.query("EXPLAIN " + query)[0][0]
        connection.send(b"Q", query.encode() + b"\0")
        deadline = time.monotonic() + 5
        while not spill_files(server) - before:
            if time.monotonic() >= deadline:
                raise AssertionError("the cancel test never observed active spill files")
            time.sleep(0.003)
        cancel_request(connection)
        assert spill_files(server) == before
        connection.query("ROLLBACK")
        assert connection.query(query) == [["3000"]]


def peak_temp_bytes(connection, query):
    plan = connection.query("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query)
    return json.loads(plan[0][0])[0]["Native Peak Temp Bytes"]


def explain_plan_nodes(connection, query):
    result = connection.query("EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) " + query)
    summary = json.loads(result[0][0])[0]
    pending = [summary["Plan"]]
    nodes = []
    while pending:
        node = pending.pop()
        nodes.append(node)
        pending.extend(node.get("Plans", ()))
    return nodes, summary["Native Peak Temp Bytes"]


def hash_join_metrics(connection, query):
    nodes, temp = explain_plan_nodes(connection, query)
    for node in nodes:
        if node["Node Type"] == "Hash Join":
            return node, temp
    raise AssertionError(nodes[0])


def verify_hash_join_metrics(connection, query):
    memory, temp = hash_join_metrics(connection, query)
    assert temp == memory["Native Hash Spill Batches"] == 0, memory
    assert memory["Native Hash Build Blocks"] == 1, memory
    assert memory["Native Hash Candidates"] >= 3000, memory
    connection.query("SET LOCAL work_mem = '64kB'")
    spilled, temp = hash_join_metrics(connection, query)
    assert temp > 0, spilled
    assert 1 <= spilled["Native Hash Spill Batches"] <= 8, spilled
    assert spilled["Native Hash Build Blocks"] >= spilled["Native Hash Spill Batches"], spilled
    assert 0 < spilled["Native Hash Peak Build Bytes"] <= 64 * 1024, spilled
    assert spilled["Native Hash Peak Build Bytes"] < memory["Native Hash Peak Build Bytes"], spilled
    assert spilled["Native Hash Candidates"] == memory["Native Hash Candidates"], spilled


def verify_hash_spill_cost(connection):
    source = "FROM hash_left l JOIN hash_right r ON l.a = r.b"
    projection = "SELECT l.id, r.id " + source
    aggregate = "SELECT count(*) " + source + " LIMIT 1"
    costs = []
    for work_mem in ("4MB", "64kB"):
        connection.query(f"SET work_mem = '{work_mem}'")
        node, temp = hash_join_metrics(connection, aggregate)
        assert (temp > 0) == (work_mem == "64kB"), node
        assert connection.query(aggregate) == [["7200"]]
        costs.append((node["Startup Cost"], node["Total Cost"]))
        limited = projection + " LIMIT 1"
        plan = connection.query("EXPLAIN " + limited)[0][0]
        assert ("Nested Loop" if work_mem == "64kB" else "Hash Inner Join") in plan, plan
        [[left, right]] = connection.query(limited)
        assert int(left) % 200 == int(right) % 200
        ordered = projection + " ORDER BY l.id, r.id LIMIT 1"
        plan = connection.query("EXPLAIN " + ordered)[0][0]
        assert "Hash Inner Join" in plan, plan
        assert connection.query(ordered) == [["0", "0"]]
    assert all(spill > fits for fits, spill in zip(*costs)), costs
    connection.query("RESET work_mem")


def verify_hash_rescans(server, connection):
    query = (
        "SELECT limits.cut, looked.n FROM "
        "(SELECT 0 AS cut UNION ALL SELECT 100 UNION ALL SELECT 201) limits "
        "CROSS JOIN LATERAL (SELECT count(*) AS n FROM hash_left l JOIN hash_right r ON l.a = r.b "
        "WHERE l.id < limits.cut) looked ORDER BY limits.cut"
    )
    before = spill_files(server)
    for work_mem in ("4MB", "64kB"):
        connection.query(f"SET work_mem = '{work_mem}'")
        assert connection.query(query) == [["0", "0"], ["100", "600"], ["201", "1206"]]
        node, temp = hash_join_metrics(connection, query)
        assert node["Actual Loops"] == 3, node
        assert (temp > 0) == (work_mem == "64kB"), node
        assert spill_files(server) == before
    connection.query("RESET work_mem")


def verify_hash_collisions_and_limit(server, connection, query):
    before = spill_files(server)
    server.control(
        "(let ((original (symbol-function 'pg::hash-join-row-hash))) "
        "  (setf (symbol-value 'pg::*hash-join-test-original-function*) original "
        "        (symbol-function 'pg::hash-join-row-hash) "
        "        (lambda (&rest args) (when (apply original args) 0))))"
    )
    try:
        for work_mem in ("4MB", "64kB"):
            connection.query(f"SET work_mem = '{work_mem}'")
            assert connection.query(query) == [["3000"]]
            node, temp = hash_join_metrics(connection, query)
            assert node["Native Hash Candidates"] == 1200 * 1200, node
            assert node["Native Hash Spill Batches"] == (0 if work_mem == "4MB" else 1), node
            assert (temp > 0) == (work_mem == "64kB"), temp
        limited = query.replace("count(*)", "l.id, r.id") + " LIMIT 1"
        [[left, right]] = connection.query(limited)
        left, right = int(left), int(right)
        assert left < right and left % 200 == right % 200, (left, right)
        node, temp = hash_join_metrics(connection, limited)
        assert temp > 0 and node["Actual Rows"] == 1, node
        assert spill_files(server) == before
    finally:
        server.control(
            "(progn (setf (symbol-function 'pg::hash-join-row-hash) pg::*hash-join-test-original-function*) "
            "       (makunbound 'pg::*hash-join-test-original-function*))"
        )
        connection.query("RESET work_mem")


def verify_spill_probe_filter(connection):
    connection.query("CREATE TABLE hash_probe_misses (k int)")
    for start in range(0, 10000, 500):
        rows = ",".join(f"({row})" for row in range(start, start + 500))
        connection.query("INSERT INTO hash_probe_misses VALUES " + rows)
    connection.query("ANALYZE hash_probe_misses")
    connection.query("SET work_mem = '64kB'")
    summary = {}
    for kind, rows in (("INNER", "1200"), ("LEFT", "11000")):
        query = (
            f"SELECT count(*), count(r.id) FROM hash_probe_misses p {kind} JOIN hash_right r "
            "ON p.k = r.b"
        )
        assert connection.query(query) == [[rows, "1200"]]
        node, summary[kind] = hash_join_metrics(connection, query)
        assert node["Build Side"] == "Right", node
        assert node["Native Hash Spill Batches"] > 0, node
    # The compact probe records occupy nine bytes each. Measure how many
    # misses the filter avoids writing independently of the build record size.
    assert 0 < summary["INNER"] <= summary["LEFT"] - 9000 * 9, summary
    connection.query("RESET work_mem")
    connection.query("DROP TABLE hash_probe_misses")


@contextlib.contextmanager
def pause_hash_join_phase(server, function, calls, condition):
    """Stop a disposable backend at a specific executor boundary."""
    marker = server.root / "hash-join-cancel-ready"
    resume = server.root / "hash-join-resume"
    symbol = f"pg::{function}"
    original = "pg::*hash-join-test-original-function*"
    iterators = "pg::*hash-join-test-iterators*"
    server.control(
        f"(let ((original (symbol-function '{symbol})) (remaining {calls})) "
        f"  (setf (symbol-value '{original}) original "
        f"        (symbol-function '{symbol}) "
        "        (lambda (&rest args) "
        f"          (when (and {condition} (zerop (decf remaining))) "
        f"            (setf (symbol-value '{iterators}) pg::*sql-open-iterators*) "
        f"            (with-open-file (marker {json.dumps(str(marker))} :direction :output) "
        '              (write-line "ready" marker)) '
        f"            (loop until (probe-file {json.dumps(str(resume))}) "
        "                  do (pg::check-for-interrupts) (sleep 0.005))) "
        "          (apply original args))))"
    )
    try:
        yield marker, resume
    finally:
        resume.touch()
        server.control(
            f"(progn (setf (symbol-function '{symbol}) (symbol-value '{original})) "
            f"       (makunbound '{original}) (makunbound '{iterators}))"
        )
        marker.unlink(missing_ok=True)
        resume.unlink(missing_ok=True)


def wait_for_pause(marker, phase):
    deadline = time.monotonic() + 5
    while not marker.exists():
        assert time.monotonic() < deadline, phase
        time.sleep(0.005)


def verify_spill_decode_failure(server, connection, query):
    before = spill_files(server)
    with pause_hash_join_phase(server, "load-hash-join-build-block", 1, "t") as (marker, resume):
        connection.send(b"Q", query.encode() + b"\0")
        wait_for_pause(marker, "before replay decode")
        files = spill_files(server) - before
        assert files
        for path in files:
            with path.open("r+b") as spill:
                spill.write(b"#<")
        resume.touch()
        error = failed_query(connection)
        assert error.sqlstate == "XX000", error
        assert spill_files(server) == before
        assert "LIVE=0" in server.control(
            "(format t \"~&LIVE=~d~%\" (hash-table-count pg::*hash-join-test-iterators*))"
        )
    assert connection.query(query) == [["3000"]]


def verify_phase_cancellation(server, query, expected):
    unjoined = "(and (null (fifth args)) (simple-vector-p (third args)))"
    cases = (
        ("build", "retain-hash-join-row", 16, "t", "4MB"),
        ("spill", "append-hash-join-partition", 16, "t", "64kB"),
        ("repartition", "repartition-hash-join-side", 2, "t", "64kB"),
        ("load", "load-hash-join-build-block", 1, "t", "64kB"),
        ("probe", "probe-hash-join-entry", 1, "t", "64kB"),
        ("unmatched build", "emit-hash-join-row", 1,
         f"(and {unjoined} (zerop (length (third args))))", "64kB"),
        ("unmatched probe", "emit-hash-join-row", 1,
         f"(and {unjoined} (plusp (length (third args))))", "64kB"),
    )
    before = spill_files(server)
    for phase, function, calls, condition, work_mem in cases:
        with contextlib.closing(CancelConnection(server.port)) as connection:
            connection.query(f"SET work_mem = '{work_mem}'")
            with pause_hash_join_phase(server, function, calls, condition) as (marker, _):
                connection.send(b"Q", query.encode() + b"\0")
                wait_for_pause(marker, phase)
                cancel_request(connection)
                assert spill_files(server) == before, phase
                output = server.control(
                    "(format t \"~&LIVE=~d~%\" "
                    "  (hash-table-count pg::*hash-join-test-iterators*))"
                )
                assert "LIVE=0" in output, (phase, output)
            assert connection.query(query) == [expected], phase


def verify_adaptive_full_join(server, connection):
    for name, lower in (("split_left", 0), ("split_right", 2000)):
        connection.query(f"CREATE TABLE {name} (k int, id int)")
        for start in range(lower, lower + 4000, 500):
            values = ",".join(
                f"({key},{key if lower == 0 else key + (1 if key % 2 == 0 else -1)})"
                for key in range(start, start + 500)
            )
            connection.query(f"INSERT INTO {name} VALUES " + values)
        connection.query(f"INSERT INTO {name} VALUES (NULL, 7000)")
        connection.query(f"ANALYZE {name}")
    projection = "SELECT count(*), count(l.id), count(r.id), count(l.k), count(r.k) FROM "
    queries = [
        projection + inputs + " ON l.k = r.k AND l.id < r.id"
        for inputs in ("split_left l FULL JOIN split_right r", "split_right r FULL JOIN split_left l")
    ]
    expected = ["7002", "4001", "4001", "4000", "4000"]
    for query in queries:
        for work_mem in ("4MB", "64kB"):
            connection.query(f"SET work_mem = '{work_mem}'")
            assert connection.query(query) == [expected]
            node, temp = hash_join_metrics(connection, query)
            assert node["Native Hash Candidates"] >= 2000, node
            if work_mem == "64kB":
                assert node["Native Hash Spill Batches"] > 8 and temp > 0, node
            else:
                assert temp == 0, node
    server.control("(setf pg::*query-temp-file-limit-bytes* 4194304)")
    try:
        assert connection.query(queries[0]) == [expected]
        node, temp = hash_join_metrics(connection, queries[0])
        assert node["Native Hash Spill Batches"] == 8 and 0 < temp < 4194304, node
    finally:
        server.control("(setf pg::*query-temp-file-limit-bytes* nil)")
    verify_phase_cancellation(server, queries[0], expected)
    connection.query("RESET work_mem")
    connection.query("DROP TABLE split_left, split_right")


def verify_left_join(connection, memory, spilling):
    connection.query(f"SET work_mem = '{memory}'")
    source = "FROM hash_left l LEFT JOIN hash_right r ON l.a = r.b AND l.id < r.id"
    query = "SELECT count(*), count(r.id) " + source
    assert "Hash Left Join" in connection.query("EXPLAIN " + query)[0][0]
    assert connection.query(query) == [["3201", "3000"]]
    assert connection.query("SELECT count(*) " + source + " WHERE r.id IS NULL") == [["201"]]
    assert (peak_temp_bytes(connection, query) > 0) == spilling


def verify_right_and_full_joins(connection):
    connection.query("INSERT INTO hash_left VALUES (1201, 998)")
    connection.query("INSERT INTO hash_right (id, b) VALUES (1201, 999)")
    for memory in ("4MB", "64kB"):
        connection.query(f"SET work_mem = '{memory}'")
        for kind, predicate, expected in (
            ("Right", "l.a = r.b AND l.id < r.id", ["3202", "3000", "3202"]),
            ("Full", "l.a = r.b", ["7204", "7202", "7202"]),
        ):
            query = (f"SELECT count(*), count(l.id), count(r.id) FROM hash_left l "
                     f"{kind} JOIN hash_right r ON {predicate}")
            assert f"Hash {kind} Join" in connection.query("EXPLAIN " + query)[0][0]
            assert connection.query(query) == [expected]
            assert (peak_temp_bytes(connection, query) > 0) == (memory == "64kB")


def verify_smaller_build_side(server):
    with contextlib.closing(fixture["Connection"](server.port)) as connection:
        connection.query("DELETE FROM hash_left WHERE id >= 200 AND id < 1200")
        connection.query("ANALYZE hash_left")
        connection.query("ANALYZE hash_right")
        connection.query("SET work_mem = '64kB'")
        for join, expected in (
            ("INNER", ["1000", "1000", "1000"]),
            ("LEFT", ["1002", "1002", "1000"]),
            ("RIGHT", ["1202", "1000", "1202"]),
            ("FULL", ["1204", "1002", "1202"]),
        ):
            query = (
                f"SELECT count(*), count(l.id), count(r.id) FROM hash_left l {join} "
                "JOIN hash_right r ON l.a = r.b AND l.id < r.id"
            )
            plan = connection.query("EXPLAIN (FORMAT JSON) " + query)[0][0]
            assert '"Build Side":"Left"' in plan, plan
            assert connection.query(query) == [expected]
            assert peak_temp_bytes(connection, query) == 0
        for start in (200, 400):
            rows = ",".join(f"({row},{row % 200})" for row in range(start, start + 200))
            connection.query("INSERT INTO hash_left VALUES " + rows)
        connection.query("ANALYZE hash_left")
        plan = connection.query("EXPLAIN (FORMAT JSON) " + query)[0][0]
        assert '"Build Side":"Left"' in plan, plan
        assert connection.query(query) == [["2604", "2402", "2602"]]
        assert peak_temp_bytes(connection, query) > 0


def verify_width_aware_build_side(server):
    def retained_width():
        output = server.control(
            "(let ((pg::*backend* (pg::make-backend :database \"postgres\"))) "
            "  (format t \"~&WIDTH=~d~%\" "
            "    (pg::pg-table-statistics-average-tuple-bytes "
            "      (pg::pg-class-statistics (pg::lookup-table \"width_wide\")))))"
        )
        return int(output.split("WIDTH=", 1)[1].split()[0])

    with contextlib.closing(fixture["Connection"](server.port)) as connection:
        connection.query("CREATE TABLE width_wide (id int, payload text)")
        connection.query("CREATE TABLE width_narrow (id int)")
        payload = "x" * 1500
        for start in range(0, 100, 20):
            values = ",".join(f"({row},'{payload}')" for row in range(start, start + 20))
            connection.query("INSERT INTO width_wide VALUES " + values)
        # Put the dead pages before the live tail so total heap size would
        # rank the narrow build as larger if the planner ignored ANALYZE.
        for start in range(400, 8400, 200):
            values = ",".join(f"({row})" for row in range(start, start + 200))
            connection.query("INSERT INTO width_narrow VALUES " + values)
        connection.query("INSERT INTO width_narrow VALUES " + ",".join(f"({row})" for row in range(400)))
        connection.query("DELETE FROM width_narrow WHERE id >= 400")
        connection.query("ANALYZE width_wide")
        connection.query("ANALYZE width_narrow")

    width_before_restart = retained_width()
    assert width_before_restart >= 1500
    server.stop(immediate=True)
    server.start()
    assert retained_width() == width_before_restart
    with contextlib.closing(fixture["Connection"](server.port)) as connection:
        connection.query("SET work_mem = '128kB'")
        query = ("SELECT w.id, length(w.payload) FROM width_wide w "
                 "JOIN width_narrow n ON w.id = n.id")
        for width in (1500, 6400):
            if width > 1500:
                payload = "".join(random.Random(19).choices("0123456789abcdefghijklmnopqrstuvwxyz", k=width))
                connection.query("ALTER TABLE width_wide ALTER COLUMN payload SET STORAGE EXTERNAL")
                connection.query(f"UPDATE width_wide SET payload = '{payload}'")
                connection.query("ANALYZE width_wide")
                assert retained_width() < 100
            plan = connection.query("EXPLAIN (FORMAT JSON) " + query)[0][0]
            assert '"Build Side":"Right"' in plan, plan
            assert sorted((int(row), int(length)) for row, length in connection.query(query)) == [
                (row, width) for row in range(100)
            ]
            peak = peak_temp_bytes(connection, query)
            assert peak == 0, (width, peak, plan)


def populate_reference_keys(connection, name, key_type, literals):
    connection.query(f"CREATE TABLE {name} (id int, k {key_type})")
    rows = ",".join(f"({idx},{literals[idx % len(literals)]})" for idx in range(320))
    connection.query(f"INSERT INTO {name} VALUES " + rows)
    connection.query(f"ANALYZE {name}")


def verify_reference_lateral(connection, reference):
    query = (
        "SELECT l.id, r.id FROM diff_left l LEFT JOIN LATERAL "
        "(SELECT d.id FROM diff_right d WHERE d.k = l.k AND d.id > l.id ORDER BY d.id LIMIT 1) r "
        "ON true ORDER BY l.id"
    )
    for work_mem in ("4MB", "64kB"):
        for peer in (connection, reference):
            peer.query(f"SET work_mem = '{work_mem}'")
        plan = connection.query("EXPLAIN " + query)[0][0]
        assert "Nested Loop" in plan and "Hash Join" not in plan, plan
        assert connection.query(query) == reference.query(query), work_mem
    for peer in (connection, reference):
        for start in range(320, 4320, 400):
            rows = ",".join(f"({i},{i})" for i in range(start, start + 400))
            peer.query("INSERT INTO diff_right VALUES " + rows)
        peer.query("CREATE INDEX diff_right_probe ON diff_right(k)")
        peer.query("ANALYZE diff_right")
    for predicate in ("d.k = l.k", "l.k = d.k"):
        indexed = query.replace("d.k = l.k", predicate)
        plan = connection.query("EXPLAIN " + indexed)[0][0]
        assert "Index Scan using diff_right_probe" in plan, plan
        assert connection.query(indexed) == reference.query(indexed), predicate
    for peer in (connection, reference):
        peer.query("DROP INDEX diff_right_probe")
        peer.query("DELETE FROM diff_right WHERE id >= 320")


def verify_reference_keys(connection, reference):
    constants = {
        "smallint": ("1", "2", "-3", "4"),
        "integer": ("1", "2000000000", "-3", "4"),
        "bigint": ("1", "9000000000000000000", "-3", "4"),
        "date": ("'2000-01-01'", "'2024-02-29'", "'1999-12-31'", "'2030-05-01'"),
        "time": ("'00:00:00'", "'12:30:10.100'", "'21:00:00'", "'01:02:03'"),
        "timestamp": ("'2000-01-01 00:00:00'", "'2024-02-29 10:00:00'",
                      "'1999-12-31 21:00:00'", "'2030-05-01 01:02:03'"),
        "timestamptz": ("'2000-01-01 00:00:00+00'", "'2024-02-29 10:00:00+00'",
                        "'1999-12-31 21:00:00+00'", "'2030-05-01 01:02:03+00'"),
        "text": ("'a'", "'A'", "'left-only'", "'right-only'"),
        "varchar": ("'a'", "'A'", "'left-only'", "'right-only'"),
        "numeric": ("1.00", "'NaN'", "-7.25", "200.0"),
    }
    source = "FROM diff_left l FULL JOIN diff_right r ON l.k = r.k AND l.id < r.id"
    queries = (
        f"SELECT l.id, r.id {source} ORDER BY l.id, r.id",
        f"SELECT l.id, r.id {source} WHERE r.id IS NULL ORDER BY l.id, r.id",
    )
    covered = (
        "SELECT count(*), count(l.id), count(r.id) FROM diff_left l "
        "FULL JOIN diff_right r ON l.k = r.k",
        "SELECT count(*), count(l.id), count(r.id) FROM diff_left l "
        "FULL JOIN diff_right r ON l.k = r.k AND l.id = r.id",
    )
    for key_type, (first, second, left_only, right_only) in constants.items():
        first_right = "1.0" if key_type == "numeric" else first
        for peer in (connection, reference):
            populate_reference_keys(peer, "diff_left", key_type, (first, second, left_only, "NULL"))
            populate_reference_keys(peer, "diff_right", key_type, (first_right, second, right_only, "NULL"))
        for work_mem in ("4MB", "64kB"):
            for peer in (connection, reference):
                peer.query(f"SET work_mem = '{work_mem}'")
            node, _ = hash_join_metrics(connection, queries[0])
            assert (node["Native Hash Spill Batches"] > 0) == (work_mem == "64kB"), (key_type, node)
            for query in queries:
                plan = connection.query("EXPLAIN " + query)[0][0]
                assert "Hash Full Join" in plan, (key_type, plan)
                assert connection.query(query) == reference.query(query), (key_type, work_mem, query)
            for query in covered:
                node, _ = hash_join_metrics(connection, query)
                assert (node["Native Hash Spill Batches"] > 0) == (work_mem == "64kB"), (key_type, node)
                assert connection.query(query) == reference.query(query), (key_type, work_mem, query)
        if key_type == "integer":
            verify_reference_lateral(connection, reference)
            for statement in ("UPDATE diff_left SET k = NULL", "DELETE FROM diff_left"):
                for peer in (connection, reference):
                    peer.query(statement)
                for query in queries:
                    assert connection.query(query) == reference.query(query), (statement, query)
        for peer in (connection, reference):
            peer.query("DROP TABLE diff_left, diff_right")
    connection.query("RESET work_mem")
    print("88 join differential cases matched PostgreSQL C", flush=True)


def verify_compact_hash_count(server, connection, reference=None, key_type="int4"):
    expected = [["1619"]]
    query = "SELECT count(*) FROM compact_probe p JOIN compact_build b ON p.k = b.k"
    before = spill_files(server)
    width = 4 if key_type == "int4" else 8
    offset = 0 if width == 4 else 1 << 62
    low, high = -(1 << (width * 8 - 1)), (1 << (width * 8 - 1)) - 1
    for peer in (connection, reference):
        if peer is None:
            continue
        peer.query("SET work_mem = '64kB'")
        for table, start_key, extra in (
            ("compact_build", 0, [None, low, low, high, offset + 7, offset + 7]),
            ("compact_probe", 1600, [None, low, low, low, high, *([offset + 7] * 4)]),
        ):
            peer.query(f"CREATE TABLE {table} (k {key_type})")
            keys = list(range(offset + start_key, offset + start_key + 3200)) + extra
            for start in range(0, len(keys), 500):
                rows = ",".join(f"({key if key is not None else 'NULL'})" for key in keys[start:start + 500])
                peer.query(f"INSERT INTO {table} VALUES " + rows)
            peer.query(f"ANALYZE {table}")
        assert peer.query(query) == expected
        assert peer.query(query + " AND p.k >= 0") == [["1613"]]
    plan, temp = hash_join_metrics(connection, query)
    assert plan["Actual Rows"] == 1619 and temp > 0, plan
    assert plan["Native Hash Peak Build Bytes"] == (48 if width == 4 else 40) * 1024, plan
    assert plan["Native Hash Key Probes"] == 3208, plan
    assert plan["Native Spill Written Bytes"] == (3205 + 3208) * width, plan
    assert spill_files(server) == before
    if key_type == "int8":
        connection.query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        try:
            assert connection.query(query) == expected
            with contextlib.closing(fixture["Connection"](server.port)) as writer:
                writer.query(f"INSERT INTO compact_build VALUES ({offset + 3200})")
            snapshot_plan, _ = hash_join_metrics(connection, query)
            assert snapshot_plan["Native Hash Key Probes"] == 3208, snapshot_plan
            assert snapshot_plan["Actual Rows"] == 1619, snapshot_plan
            connection.query(f"DELETE FROM compact_build WHERE k = {offset + 1600}")
            assert connection.query(query) == [["1618"]]
        finally:
            connection.query("ROLLBACK")
        assert connection.query(query) == [["1620"]]
        connection.query(f"DELETE FROM compact_build WHERE k = {offset + 3200}")
    with contextlib.closing(CancelConnection(server.port)) as active:
        active.query("SET work_mem = '64kB'")
        with pause_hash_join_phase(server, f"map-{key_type}-hash-spill-keys", 1, "t") as (marker, _):
            active.send(b"Q", query.encode() + b"\0")
            wait_for_pause(marker, "before compact hash replay")
            assert spill_files(server) - before
            cancel_request(active)
            assert spill_files(server) == before
        assert active.query(query) == expected
    for table in ("compact_build", "compact_probe"):
        connection.query(f"UPDATE {table} SET k = -5 WHERE k IS NOT NULL")
    expected_skew = 3205 * 3208
    assert connection.query(query) == [[str(expected_skew)]]
    skew, _ = hash_join_metrics(connection, query)
    probes = 3208 if skew["Build Side"] == "Right" else 3205
    assert skew["Actual Rows"] == expected_skew and skew["Native Hash Key Probes"] == probes, skew
    assert spill_files(server) == before
    for peer in (connection, reference):
        if peer is not None:
            peer.query("DROP TABLE compact_build, compact_probe")
            peer.query("RESET work_mem")


def verify_compact_temporal_counts(connection, reference):
    epoch = dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc)
    east = dt.timezone(dt.timedelta(hours=5, minutes=30))

    def literal(type_name, i, right):
        instant = epoch + dt.timedelta(seconds=2 * i, microseconds=17 * i)
        if type_name == "date":
            value = epoch.date() + dt.timedelta(days=i)
        elif type_name == "time":
            value = instant.time()
        elif type_name == "timestamp":
            value = instant.replace(tzinfo=None)
        else:
            value = instant.astimezone(east) if right else instant
        return "'" + value.isoformat() + "'"

    query = "SELECT count(*) FROM temporal_probe p JOIN temporal_build b ON p.k = b.k"
    for type_name, width in (("date", 4), ("time", 8), ("timestamp", 8), ("timestamptz", 8)):
        for peer in (connection, reference):
            peer.query("SET work_mem = '64kB'")
            for table, start, right in (("temporal_probe", 0, False), ("temporal_build", 1200, True)):
                peer.query(f"CREATE TABLE {table} (k {type_name})")
                keys = [literal(type_name, i, right) for i in range(start, start + 2400)]
                keys.extend([literal(type_name, 1200, right), "NULL"])
                for offset in range(0, len(keys), 400):
                    rows = ",".join(f"({key})" for key in keys[offset:offset + 400])
                    peer.query(f"INSERT INTO {table} VALUES " + rows)
                peer.query(f"ANALYZE {table}")
            assert peer.query(query) == [["1203"]], type_name
        plan, _ = hash_join_metrics(connection, query)
        assert "Native Hash Key Probes" in plan and plan["Actual Rows"] == 1203, plan
        assert plan["Native Hash Peak Build Bytes"] == (48 if width == 4 else 40) * 1024, plan
        for peer in (connection, reference):
            peer.query("DROP TABLE temporal_probe, temporal_build")
            peer.query("RESET work_mem")


def verify_fixed_float_counts(connection, reference):
    query = "SELECT count(*) FROM fixed_probe p JOIN fixed_build b ON p.k = b.k"
    rows_query = "SELECT count(p.k) FROM fixed_probe p JOIN fixed_build b ON p.k = b.k"
    for type_name, width in (("float4", 4), ("float8", 8)):
        exceptional = ["'Infinity'", "'-Infinity'", "'NaN'", "'-0.0'", "'0.0'"]
        expected = 1207
        for peer in (connection, reference):
            peer.query("SET work_mem = '64kB'")
            for table, start in (("fixed_probe", 0), ("fixed_build", 1200)):
                peer.query(f"CREATE TABLE {table} (k {type_name})")
                keys = [str(i + 0.125) for i in range(start, start + 2400)]
                keys.extend([*exceptional, "NULL"])
                for offset in range(0, len(keys), 400):
                    rows = ",".join(f"({value})" for value in keys[offset:offset + 400])
                    peer.query(f"INSERT INTO {table} VALUES " + rows)
                peer.query(f"ANALYZE {table}")
            assert peer.query(query) == [[str(expected)]], type_name
            assert peer.query(rows_query) == [[str(expected)]], type_name
        plan, _ = hash_join_metrics(connection, query)
        assert "Native Hash Key Probes" in plan and plan["Actual Rows"] == expected, plan
        assert plan["Native Hash Peak Build Bytes"] == (48 if width == 4 else 40) * 1024, plan
        generic, _ = hash_join_metrics(connection, rows_query)
        assert generic["Native Hash Spill Batches"] > 0 and generic["Actual Rows"] == expected, generic
        for peer in (connection, reference):
            peer.query("DROP TABLE fixed_probe, fixed_build")
            peer.query("RESET work_mem")


def verify_interval_hash_equality(connection, reference):
    query = "SELECT count(*) FROM span_probe p JOIN span_build b ON p.k = b.k"
    rows_query = "SELECT count(p.k) FROM span_probe p JOIN span_build b ON p.k = b.k"
    for peer in (connection, reference):
        peer.query("SET work_mem = '64kB'")
        for table, start in (("span_probe", 0), ("span_build", 1200)):
            peer.query(f"CREATE TABLE {table} (k interval)")
            equivalent = ("1 month", "30 days") if start == 0 else ("720 hours", "30 days")
            keys = [f"'{i - 1800} microseconds'" for i in range(start, start + 2400)]
            keys.extend(f"'{span}'" for span in (*equivalent, "2147483647 months", "-2147483648 days"))
            keys.append("NULL")
            for offset in range(0, len(keys), 400):
                rows = ",".join(f"({value})" for value in keys[offset:offset + 400])
                peer.query(f"INSERT INTO {table} VALUES " + rows)
            peer.query(f"ANALYZE {table}")
        assert peer.query(query) == [["1206"]]
        assert peer.query(rows_query) == [["1206"]]
    plan, _ = hash_join_metrics(connection, query)
    assert plan["Native Hash Peak Build Bytes"] == 48 * 1024 and plan["Actual Rows"] == 1206, plan
    generic, _ = hash_join_metrics(connection, rows_query)
    assert generic["Native Hash Spill Batches"] > 0 and generic["Actual Rows"] == 1206, generic
    for peer in (connection, reference):
        peer.query("DROP TABLE span_probe, span_build")
        peer.query("RESET work_mem")


def verify_interval_hash_skew(connection, reference):
    query = "SELECT count(*) FROM span_probe p JOIN span_build b ON p.k = b.k"
    for peer in (connection, reference):
        peer.query("SET work_mem = '64kB'")
        for table in ("span_probe", "span_build"):
            peer.query(f"CREATE TABLE {table} (k interval)")
            # These distinct spans share the interval router's mixed 64 bits.
            rows = ",".join(["('0 seconds')", "('1 day 01:11:34.967296')"] * 200)
            for _ in range(9):
                peer.query(f"INSERT INTO {table} VALUES " + rows)
            peer.query(f"ANALYZE {table}")
        assert peer.query(query) == [["6480000"]]
    plan, _ = hash_join_metrics(connection, query)
    assert plan["Native Hash Build Blocks"] == 2, plan
    assert plan["Native Hash Spill Batches"] <= 4, plan
    for peer in (connection, reference):
        peer.query("DROP TABLE span_probe, span_build")
        peer.query("RESET work_mem")


def main(reference_bin=None):
    server = fixture["OwnedLisp"]()
    try:
        server.start()
        with contextlib.closing(fixture["Connection"](server.port)) as connection:
            if reference_bin is not None:
                reference_server = runpy.run_path(str(Path(__file__).with_name("btree-range.py")))["reference_server"]
                with reference_server(reference_bin) as reference:
                    verify_reference_keys(connection, reference)
                    verify_compact_hash_count(server, connection, reference)
                    verify_compact_hash_count(server, connection, reference, key_type="int8")
                    verify_compact_temporal_counts(connection, reference)
                    verify_fixed_float_counts(connection, reference)
                    verify_interval_hash_equality(connection, reference)
                    verify_interval_hash_skew(connection, reference)
            else:
                verify_compact_hash_count(server, connection)
                verify_compact_hash_count(server, connection, key_type="int8")
            query = verify_join(connection)
            assert connection.query("SHOW work_mem") == [["4MB"]]
            connection.query("BEGIN")
            verify_hash_join_metrics(connection, query)
            connection.query("ROLLBACK")
            verify_hash_spill_cost(connection)
            verify_hash_rescans(server, connection)
            verify_hash_collisions_and_limit(server, connection, query)
            verify_spill_probe_filter(connection)
            verify_adaptive_full_join(server, connection)
            connection.query("BEGIN")
            connection.query("SET LOCAL work_mem = 64")
            assert connection.query("SHOW work_mem") == [["64kB"]]
            assert peak_temp_bytes(connection, query) > 0
            connection.query("ROLLBACK")
            assert connection.query("SHOW work_mem") == [["4MB"]]
            connection.query("BEGIN")
            connection.query("SET LOCAL work_mem = '128kB'")
            connection.query("SET work_mem = '256kB'")
            connection.query("COMMIT")
            assert connection.query("SHOW work_mem") == [["256kB"]]
            connection.query("SET work_mem = '64kB'")
            assert connection.query(
                "SELECT setting, unit FROM pg_settings WHERE name = 'work_mem'"
            ) == [["64", "kB"]]
            with contextlib.closing(fixture["Connection"](server.port)) as observer:
                assert observer.query("SHOW work_mem") == [["4MB"]]
            assert connection.query(query) == [["3000"]]
            verify_suspended_join(server, connection)
            verify_early_close(server, connection)
            verify_disconnect_cleanup(server)
            verify_spill_quota_cleanup(server, connection, query)
            verify_spill_decode_failure(server, connection, query)
            verify_cancel_cleanup(server, query)
            verify_left_join(connection, "4MB", False)
            verify_left_join(connection, "64kB", True)
            verify_right_and_full_joins(connection)
            payload = "".join(random.Random(19).choices("0123456789abcdefghijklmnopqrstuvwxyz", k=100000))
            connection.query(f"UPDATE hash_right SET payload = '{payload}' WHERE id = 207")
            wide = query.replace("count(*)", "count(*), max(length(r.payload))")
            assert "Hash Inner Join" in connection.query("EXPLAIN " + wide)[0][0]
            assert connection.query(wide) == [["3000", "100000"]]
            connection.query("RESET work_mem")
            assert connection.query("SHOW work_mem") == [["4MB"]]
            selective = (
                "SELECT r.id FROM hash_left l JOIN hash_right r ON l.a = r.b "
                "WHERE l.id = 7 ORDER BY r.id"
            )
            connection.query("CREATE INDEX hash_left_id ON hash_left(id)")
            connection.query("CREATE INDEX hash_right_key ON hash_right(b)")
            plan = connection.query("EXPLAIN " + selective)[0][0]
            assert "Nested Loop" in plan and "Index Scan" in plan, plan
            assert connection.query(selective) == [[str(row)] for row in range(7, 1200, 200)]
            connection.query("DROP INDEX hash_left_id, hash_right_key")
        verify_crash_cleanup(server, query)
        verify_smaller_build_side(server)
        verify_width_aware_build_side(server)
    finally:
        server.stop()
        shutil.rmtree(server.root)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-bin", type=Path, help="run differential cases against these PostgreSQL binaries")
    main(parser.parse_args().reference_bin)
