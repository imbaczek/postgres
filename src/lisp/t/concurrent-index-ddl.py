"""Exercise concurrent index phase boundaries over the PostgreSQL wire."""

import argparse
import contextlib
import os
import random
import re
import runpy
import struct
import subprocess
import time
from importlib import import_module
from pathlib import Path

fixture = runpy.run_path(str(Path(__file__).with_name("command-complete.py")))
OwnedLisp = fixture["OwnedLisp"]
Connection = fixture["Connection"]
wait_for_file = fixture["wait_for_file"]
WireError = import_module("parking_wire").WireError
LISP = fixture["LISP"]
parse, bind, frame = (fixture[name] for name in ("parse", "bind", "frame"))


def expect_error(connection, statement, sqlstate):
    try:
        connection.query(statement)
    except WireError as error:
        assert error.sqlstate == sqlstate, (statement, error)
    else:
        raise AssertionError(f"statement unexpectedly succeeded: {statement}")


def arm_phase(server, phase):
    server.control(f'(setf pg::*wal-test-pause-at* "{phase}")')


def phase_file(server, phase, action):
    return server.data / "pg_wal" / f"test-{action}-{phase}"


def release_phase(server, phase):
    if phase_file(server, phase, "paused").exists():
        phase_file(server, phase, "resume").touch()


def index_state(connection, name):
    return connection.query(
        "SELECT indisvalid, indisready, indislive FROM pg_catalog.pg_index "
        "WHERE indexrelid=(SELECT oid FROM pg_catalog.pg_class "
        f"WHERE relname='{name}')"
    )


def mark_logical_catalog_wal(server):
    server.control("(defparameter pg::*test-index-catalog-wal-start* "
                   "(sb-thread:with-mutex (pg::*wal-lock*) pg::*wal-durable-lsn*))")


def logical_catalog_records_since_mark(server):
    output = server.control("""(sb-thread:with-mutex (pg::*wal-lock*)
      (let ((count 0))
        (pg::wal-scan (lambda (record)
                        (when (= (pg::wal-record-type record) pg::+wal-record-logical-catalog-event+)
                          (incf count)))
                      pg::*test-index-catalog-wal-start* :end-lsn pg::*wal-durable-lsn*)
        (format nil "CATALOG-RECORDS=~d" count)))""")
    return int(re.search(r"CATALOG-RECORDS=(\d+)", output).group(1))


def assert_index_key_entries(server, index, key, expected):
    response = server.control(
        '(let* ((pg::*backend* (pg::make-backend :database "postgres")) '
        f'   (index (pg::lookup-index "{index}")) '
        f"   (key (pg::encode-index-key (list {key}) '(:int4)))) "
        '  (format t "~&INDEX-ENTRIES=~d~%" '
        "          (length (pg::btree-search (pg::pg-index-relfilenode index) key))) "
        "  (values))"
    )
    assert f"\nINDEX-ENTRIES={expected}\n" in response, response


def utility_frames(connection):
    frames = []
    while True:
        kind, body = connection.receive()
        frames.append((kind, body))
        if kind == b"Z":
            return frames


def finish_utility(connection, tag, ready=b"I"):
    frames = utility_frames(connection)
    assert frames == [(b"C", tag + b"\0"), (b"Z", ready)], frames


def utility_error(connection, sqlstate):
    frames = utility_frames(connection)
    assert [kind for kind, _ in frames] == [b"E", b"Z"], frames
    assert frames[-1][1] == b"I", frames
    failure = WireError(connection.error_fields(frames[0][1]))
    assert failure.sqlstate == sqlstate, failure


def wait_for(description, predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.025)
    raise AssertionError(f"timed out waiting for {description}")


def wait_for_query(connection, statement, expected):
    def matches():
        try:
            return connection.query(statement) == expected
        except WireError as error:
            if error.sqlstate != "42P01":
                raise
            return False

    wait_for(statement, matches)


def wait_for_dependencies(server, connection):
    wait_for(
        "a registered relation-phase dependency",
        lambda: "\nT\n" in server.control(
            "(sb-thread:with-mutex (pg::*relation-lock-mutex*) "
            f"(not (null (gethash (pg::lookup-backend {connection.backend_pid}) "
            "pg::*relation-wait-dependencies*))))"
        ),
    )


def drop_phases(server, dropper, reader, witness):
    invalid = "after-concurrent-index-invalid"
    dead = "after-concurrent-index-dead"
    witness.query("CREATE TABLE drop_rows(id int)")
    witness.query("INSERT INTO drop_rows VALUES (1)")
    witness.query("CREATE UNIQUE INDEX drop_idx ON drop_rows(id)")
    reader.query("BEGIN")
    assert reader.query("SELECT id FROM drop_rows WHERE id=1") == [["1"]]

    arm_phase(server, invalid)
    dropper.send(b"Q", b"DROP INDEX CONCURRENTLY drop_idx\0")
    wait_for_file(phase_file(server, invalid, "paused"), server.process)
    assert index_state(witness, "drop_idx") == [["f", "t", "t"]]
    witness.query("INSERT INTO drop_rows VALUES (2)")
    expect_error(witness, "INSERT INTO drop_rows VALUES (1)", "23505")
    arm_phase(server, dead)
    phase_file(server, invalid, "resume").touch()

    wait_for_dependencies(server, dropper)
    assert witness.query("SELECT id FROM drop_rows WHERE id=2") == [["2"]]
    assert index_state(witness, "drop_idx") == [["f", "t", "t"]]
    reader.query("COMMIT")
    wait_for_file(phase_file(server, dead, "paused"), server.process)
    assert index_state(witness, "drop_idx") == [["f", "f", "f"]]
    witness.query("INSERT INTO drop_rows VALUES (1)")
    phase_file(server, dead, "resume").touch()
    finish_utility(dropper, b"DROP INDEX")
    assert index_state(witness, "drop_idx") == []
    assert witness.query("SELECT id FROM drop_rows ORDER BY id") == [
        ["1"], ["1"], ["2"]
    ]


def create_phases(server, builder, writer, witness):
    unready = "after-concurrent-index-unready"
    ready = "after-concurrent-index-ready"
    witness.query("CREATE TABLE build_rows(id int, payload int)")
    witness.query("INSERT INTO build_rows VALUES (1, 11)")
    writer.query("BEGIN")
    writer.query("INSERT INTO build_rows VALUES (2, 22)")

    arm_phase(server, unready)
    builder.send(b"Q", b"CREATE UNIQUE INDEX CONCURRENTLY build_idx ON build_rows(id)\0")
    wait_for_file(phase_file(server, unready, "paused"), server.process)
    assert index_state(witness, "build_idx") == [["f", "f", "t"]]
    witness.query("INSERT INTO build_rows VALUES (3, 33)")
    arm_phase(server, ready)
    phase_file(server, unready, "resume").touch()
    wait_for_dependencies(server, builder)
    assert index_state(witness, "build_idx") == [["f", "f", "t"]]
    writer.query("COMMIT")
    wait_for_file(phase_file(server, ready, "paused"), server.process)
    assert index_state(witness, "build_idx") == [["f", "t", "t"]]
    witness.query("INSERT INTO build_rows VALUES (4, 44)")
    expect_error(witness, "INSERT INTO build_rows VALUES (2, 99)", "23505")
    expect_error(
        witness, "INSERT INTO build_rows VALUES (2, 99) ON CONFLICT (id) DO NOTHING",
        "42P10",
    )
    phase_file(server, ready, "resume").touch()
    finish_utility(builder, b"CREATE INDEX")
    assert index_state(witness, "build_idx") == [["t", "t", "t"]]
    for value in range(1, 5):
        assert_index_key_entries(server, "build_idx", value, 1)
        assert witness.query(f"SELECT payload FROM build_rows WHERE id={value}") == [
            [str(value * 11)]
        ]


def unique_validation_failure(server, builder, witness):
    built = "after-concurrent-index-build"
    witness.query("CREATE TABLE duplicate_rows(id int)")
    witness.query("INSERT INTO duplicate_rows VALUES (1)")
    arm_phase(server, built)
    builder.send(
        b"Q",
        b"CREATE UNIQUE INDEX CONCURRENTLY duplicate_idx ON duplicate_rows(id)\0",
    )
    wait_for_file(phase_file(server, built, "paused"), server.process)
    witness.query("INSERT INTO duplicate_rows VALUES (1)")
    phase_file(server, built, "resume").touch()

    utility_error(builder, "23505")
    assert index_state(witness, "duplicate_idx") == [["f", "t", "t"]]
    expect_error(
        witness, "ALTER TABLE duplicate_rows ADD UNIQUE USING INDEX duplicate_idx",
        "55000",
    )
    witness.query("ALTER INDEX duplicate_idx RENAME TO failed_idx")
    witness.query("DROP INDEX failed_idx")
    assert index_state(witness, "failed_idx") == []


def validation_accepts_replaced_versions(server, builder, witness):
    built = "after-concurrent-index-build"
    validating = "before-concurrent-index-validation"
    witness.query("CREATE TABLE replaced_rows(id int, value int)")
    witness.query("INSERT INTO replaced_rows VALUES (1,11)")
    arm_phase(server, built)
    builder.send(
        b"Q",
        b"CREATE UNIQUE INDEX CONCURRENTLY replaced_idx ON replaced_rows(id)\0",
    )
    wait_for_file(phase_file(server, built, "paused"), server.process)
    witness.query("INSERT INTO replaced_rows VALUES (2,22),(3,33)")
    arm_phase(server, validating)
    phase_file(server, built, "resume").touch()
    wait_for_file(phase_file(server, validating, "paused"), server.process)
    witness.query("UPDATE replaced_rows SET value=23 WHERE id=2")
    witness.query("DELETE FROM replaced_rows WHERE id=3")
    witness.query("INSERT INTO replaced_rows VALUES (3,34)")
    phase_file(server, validating, "resume").touch()
    finish_utility(builder, b"CREATE INDEX")
    assert_index_key_entries(server, "replaced_idx", 2, 2)
    assert_index_key_entries(server, "replaced_idx", 3, 2)
    assert witness.query("SELECT value FROM replaced_rows WHERE id=2") == [["23"]]
    assert witness.query("SELECT value FROM replaced_rows WHERE id=3") == [["34"]]


def queued_vacuum_does_not_block_validation(server, builder, vacuum, witness):
    ready = "after-concurrent-index-ready"
    witness.query("CREATE TABLE vacuum_rows(id int)")
    witness.query("INSERT INTO vacuum_rows VALUES (1)")
    arm_phase(server, ready)
    builder.send(b"Q", b"CREATE INDEX CONCURRENTLY vacuum_idx ON vacuum_rows(id)\0")
    wait_for_file(phase_file(server, ready, "paused"), server.process)
    vacuum.send(b"Q", b"VACUUM vacuum_rows\0")
    wait_for_dependencies(server, vacuum)
    witness.query("INSERT INTO vacuum_rows VALUES (2)")
    phase_file(server, ready, "resume").touch()
    finish_utility(builder, b"CREATE INDEX")
    finish_utility(vacuum, b"VACUUM")
    assert_index_key_entries(server, "vacuum_idx", 2, 1)


def queued_read_committed_truncate_does_not_block_validation(server, builder, truncater, witness):
    ready = "after-concurrent-index-ready"
    witness.query("CREATE TABLE truncate_wait_rows(id int)")
    witness.query("INSERT INTO truncate_wait_rows VALUES (1)")
    truncater.query("BEGIN ISOLATION LEVEL READ COMMITTED")
    arm_phase(server, ready)
    builder.send(b"Q", b"CREATE INDEX CONCURRENTLY truncate_wait_idx ON truncate_wait_rows(id)\0")
    wait_for_file(phase_file(server, ready, "paused"), server.process)
    truncater.send(b"Q", b"TRUNCATE truncate_wait_rows\0")
    wait_for_dependencies(server, truncater)
    witness.query("INSERT INTO truncate_wait_rows VALUES (2)")
    phase_file(server, ready, "resume").touch()
    finish_utility(builder, b"CREATE INDEX")
    finish_utility(truncater, b"TRUNCATE TABLE", ready=b"T")
    truncater.query("COMMIT")
    assert witness.query("SELECT id FROM truncate_wait_rows") == []
    assert index_state(witness, "truncate_wait_idx") == [["t", "t", "t"]]


def admitted_upsert_survives_invalidation(server, dropper, writer, blocker, witness):
    invalid = "after-concurrent-index-invalid"
    witness.query("CREATE TABLE upsert_rows(id int)")
    witness.query("CREATE UNIQUE INDEX upsert_idx ON upsert_rows(id)")
    pid = int(writer.query("SELECT pg_backend_pid()")[0][0])
    blocker.query("BEGIN")
    blocker.query("INSERT INTO upsert_rows VALUES (10)")
    writer.send(
        b"Q",
        b"INSERT INTO upsert_rows VALUES (10),(11) ON CONFLICT (id) DO NOTHING\0",
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        result = server.control(
            f"(not (null (pg::backend-waiting-xid (pg::lookup-backend {pid}))))"
        )
        if "\nT\n" in result:
            break
        time.sleep(0.01)
    else:
        raise AssertionError("upsert did not wait for the old writer")

    arm_phase(server, invalid)
    dropper.send(b"Q", b"DROP INDEX CONCURRENTLY upsert_idx\0")
    wait_for_file(phase_file(server, invalid, "paused"), server.process)
    assert index_state(witness, "upsert_idx") == [["f", "t", "t"]]
    blocker.query("COMMIT")
    finish_utility(writer, b"INSERT 0 1")
    phase_file(server, invalid, "resume").touch()
    finish_utility(dropper, b"DROP INDEX")
    assert witness.query("SELECT id FROM upsert_rows ORDER BY id") == [["10"], ["11"]]

    witness.query("CREATE UNIQUE INDEX upsert_idx ON upsert_rows(id)")
    writer.socket.sendall(
        parse("INSERT INTO upsert_rows VALUES (10),(12) ON CONFLICT (id) DO NOTHING")
        + bind() + frame(b"H", b"")
    )
    assert writer.receive() == (b"1", b"")
    assert writer.receive() == (b"2", b"")
    dropper.send(b"Q", b"DROP INDEX CONCURRENTLY upsert_idx\0")
    wait_for_file(phase_file(server, invalid, "paused"), server.process)
    writer.socket.sendall(frame(b"E", b"\0" + struct.pack("!I", 0)) + frame(b"S", b""))
    finish_utility(writer, b"INSERT 0 1")
    phase_file(server, invalid, "resume").touch()
    finish_utility(dropper, b"DROP INDEX")
    assert witness.query("SELECT id FROM upsert_rows ORDER BY id") == [["10"], ["11"], ["12"]]


def physical_index_phases():
    source, target = OwnedLisp(), OwnedLisp()
    connections = []
    unready = "after-concurrent-index-unready"
    built = "after-concurrent-index-build"
    try:
        source.start()
        source_sql = Connection(source.port)
        connections.append(source_sql)
        source_sql.query("CREATE TABLE replay_rows(id int)")
        for start in range(1, 1501, 250):
            rows = ",".join(f"({value})" for value in range(start, start + 250))
            source_sql.query(f"INSERT INTO replay_rows VALUES {rows}")
        source_sql.query("CREATE TABLE replay_witness(id int)")
        target.data.mkdir(mode=0o700)
        source.control(f'(pg::physical-base-backup "{target.data}/" "index_phase")')
        target.start()
        target.control(f'(pg::physical-follow-start "127.0.0.1" {source.port} "index_phase")')
        builder = Connection(source.port)
        standby = Connection(target.port)
        observer = Connection(target.port)
        connections.extend((builder, standby, observer))

        arm_phase(source, unready)
        builder.send(b"Q", b"CREATE UNIQUE INDEX CONCURRENTLY replay_idx "
                          b"ON replay_rows ((id + 1)) WHERE id > 1000\0")
        wait_for_file(phase_file(source, unready, "paused"), source.process)
        wait_for("standby catalog install",
                 lambda: index_state(observer, "replay_idx") == [["f", "f", "t"]])
        standby.query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        assert standby.query("SELECT id FROM replay_rows WHERE id=1500") == [["1500"]]

        arm_phase(source, built)
        phase_file(source, unready, "resume").touch()
        wait_for_file(phase_file(source, built, "paused"), source.process)
        source_sql.query("INSERT INTO replay_witness VALUES (1)")
        position = source_sql.query("SELECT pg_current_wal_flush_lsn()")[0][0]
        wait_for_query(observer, f"SELECT pg_last_wal_receive_lsn() >= '{position}'::pg_lsn",
                       [["t"]])
        assert observer.query(
            f"SELECT pg_last_wal_replay_lsn() < '{position}'::pg_lsn"
        ) == [["t"]], "build pages were replayed through a pinned standby reader"
        phase_file(source, built, "resume").touch()
        finish_utility(builder, b"CREATE INDEX")
        assert standby.query("SELECT id FROM replay_rows WHERE id=1500") == [["1500"]]
        standby.query("COMMIT")
        wait_for("standby valid index",
                 lambda: index_state(observer, "replay_idx") == [["t", "t", "t"]])
        assert observer.query("SELECT id FROM replay_rows WHERE id=1500") == [["1500"]]
        indexed = "SELECT id FROM replay_rows WHERE id > 1000 AND id + 1 = 1501"
        assert "Index Scan using replay_idx" in observer.query("EXPLAIN " + indexed)[0][0]
        assert observer.query(indexed) == [["1500"]]
        source_sql.query("BEGIN")
        source_sql.query("CREATE TABLE replay_mixed (id int)")
        source_sql.query("INSERT INTO replay_mixed VALUES (7), (11)")
        for start in range(1000, 2500, 250):
            rows = ",".join(f"({value})" for value in range(start, start + 250))
            source_sql.query(f"INSERT INTO replay_mixed VALUES {rows}")
        source_sql.query("CREATE UNIQUE INDEX replay_mixed_ix ON replay_mixed ((id + 1))")
        source_sql.query("UPDATE replay_mixed SET id = 13 WHERE id = 11")
        expect_error(observer, "SELECT id FROM replay_mixed", "42P01")
        assert index_state(observer, "replay_mixed_ix") == []
        source_sql.query("COMMIT")
        position = source_sql.query("SELECT pg_current_wal_flush_lsn()")[0][0]
        wait_for_query(observer, f"SELECT pg_last_wal_replay_lsn() >= '{position}'::pg_lsn",
                       [["t"]])
        assert observer.query("SELECT id FROM replay_mixed WHERE id < 1000 ORDER BY id") == [
            ["7"], ["13"]
        ]
        assert observer.query("SELECT count(*) FROM replay_mixed") == [["1502"]]
        indexed = "SELECT id FROM replay_mixed WHERE id + 1 = 14"
        assert "Index Scan using replay_mixed_ix" in observer.query("EXPLAIN " + indexed)[0][0]
        assert observer.query(indexed) == [["13"]]
        assert index_state(observer, "replay_mixed_ix") == [["t", "t", "t"]]
        source_sql.query("DROP INDEX CONCURRENTLY replay_idx")
        wait_for("standby index removal", lambda: index_state(observer, "replay_idx") == [])
        print("concurrent index physical replay: PASS")
    finally:
        for phase in (unready, built):
            release_phase(source, phase)
        for connection in connections:
            connection.close()
        target.stop()
        source.stop()
        print(f"physical artifacts: {source.root}, {target.root}")


def crash_and_cancel():
    server = OwnedLisp()
    connections = []
    ready = "after-concurrent-index-ready"
    dead = "after-concurrent-index-dead"

    def connect():
        connection = Connection(server.port)
        connections.append(connection)
        return connection

    def crash_at(phase):
        wait_for_file(phase_file(server, phase, "paused"), server.process)
        server.stop(immediate=True)
        phase_file(server, phase, "paused").unlink(missing_ok=True)
        while connections:
            connections.pop().close()
        server.start()

    try:
        server.start()
        builder, witness = connect(), connect()
        witness.query("CREATE TABLE restart_rows(id int)")
        witness.query("INSERT INTO restart_rows VALUES (1)")
        arm_phase(server, ready)
        builder.send(b"Q", b"CREATE UNIQUE INDEX CONCURRENTLY restart_idx ON restart_rows(id)\0")
        crash_at(ready)

        builder, witness = connect(), connect()
        assert index_state(witness, "restart_idx") == [["f", "t", "t"]]
        expect_error(witness, "INSERT INTO restart_rows VALUES (1)", "23505")
        witness.query("DROP INDEX CONCURRENTLY restart_idx")
        witness.query("CREATE UNIQUE INDEX restart_idx ON restart_rows(id)")
        arm_phase(server, dead)
        builder.send(b"Q", b"DROP INDEX CONCURRENTLY restart_idx\0")
        crash_at(dead)

        builder, witness, reader = connect(), connect(), connect()
        assert index_state(witness, "restart_idx") == [["f", "f", "f"]]
        witness.query("INSERT INTO restart_rows VALUES (1)")
        witness.query("DROP INDEX CONCURRENTLY restart_idx")
        witness.query("CREATE INDEX restart_idx ON restart_rows(id)")
        reader.query("BEGIN")
        reader.query("SELECT id FROM restart_rows")
        pid = builder.backend_pid
        builder.send(b"Q", b"DROP INDEX CONCURRENTLY restart_idx\0")
        wait_for_dependencies(server, builder)
        assert witness.query(f"SELECT pg_cancel_backend({pid})") == [["t"]]
        utility_error(builder, "57014")
        assert index_state(witness, "restart_idx") == [["f", "t", "t"]]
        assert builder.query("SELECT 1") == [["1"]]
        reader.query("COMMIT")
        witness.query("DROP INDEX CONCURRENTLY restart_idx")
        assert index_state(witness, "restart_idx") == []
        print("concurrent index crash and cancel: PASS")
    finally:
        for phase in (ready, dead):
            release_phase(server, phase)
        for connection in connections:
            connection.close()
        server.stop()
        print(f"restart artifacts: {server.root}")


def logical_identity_cutover():
    source, target = OwnedLisp(), OwnedLisp()
    connections = []
    worker = None
    validated = "after-concurrent-index-validation"
    invalid = "after-concurrent-index-invalid"

    def connect(server):
        connection = Connection(server.port)
        connections.append(connection)
        return connection

    def start_worker(log_name, env=None):
        with (source.root / log_name).open("wb") as log:
            return subprocess.Popen(
                [str(LISP / "pg-lisp-subscribe"), "--slot", "index_phase",
                 "--source-port", str(source.port), "--target-port", str(target.port),
                 "--table", "logical_rows"],
                cwd=LISP, stdout=log, stderr=subprocess.STDOUT, env=env,
            )

    def wait_for_fence():
        wait_for(
            "logical identity writer fence",
            lambda: "\nT\n" in source.control(
                "(sb-thread:with-mutex (pg::*relation-lock-mutex*) "
                "(plusp (hash-table-count pg::*relation-builders*)))"
            ),
        )

    try:
        source.start()
        target.start()
        source_sql, builder, writer = (connect(source) for _ in range(3))
        subscriber = connect(target)
        source_sql.query("CREATE TABLE logical_rows(a int NOT NULL, b int NOT NULL, v int, "
                         "scheduled timestamptz)")
        source_sql.query("CREATE UNIQUE INDEX logical_a ON logical_rows(a)")
        source_sql.query("INSERT INTO logical_rows VALUES (1,10,100,'2026-01-01 15:00:00+00')")
        for start in range(3, 2002, 250):
            rows = ",".join(f"({value},{value + 100},0)" for value in range(start, min(start + 250, 2002)))
            source_sql.query(f"INSERT INTO logical_rows (a,b,v) VALUES {rows}")
        worker = start_worker("subscription.log")
        wait_for_query(subscriber, "SELECT count(*) FROM logical_rows", [["2000"]])
        first_row = "SELECT a,b,v FROM logical_rows WHERE a=1"
        assert subscriber.query(first_row) == [["1", "10", "100"]]

        source_sql.query("SET TIME ZONE 'America/New_York'")
        source_sql.query("CREATE UNIQUE INDEX logical_expr ON logical_rows ((v + 1)) "
                         "WHERE b > 0 AND scheduled < '2026-01-01 12:00:00'")
        wait_for("subscriber partial expression index",
                 lambda: index_state(subscriber, "logical_expr") == [["t", "t", "t"]])
        source_sql.query("INSERT INTO logical_rows VALUES (2,0,100,'2026-01-01 15:00:00+00')")
        wait_for_query(subscriber, "SELECT a,v FROM logical_rows WHERE a=2", [["2", "100"]])
        source_sql.query("DELETE FROM logical_rows WHERE a=2")
        wait_for_query(subscriber, "SELECT a,v FROM logical_rows WHERE a=2", [])
        overlapping = "INSERT INTO logical_rows VALUES (2,20,100,'2026-01-01 15:00:00+00')"
        expect_error(subscriber, overlapping, "23505")
        source_sql.query("DROP INDEX logical_expr")
        wait_for("subscriber partial expression index removal",
                 lambda: index_state(subscriber, "logical_expr") == [])
        source_sql.query("CREATE UNIQUE INDEX CONCURRENTLY logical_expr ON logical_rows "
                         "((v + 1)) WHERE b::oid8 > 0::oid8 "
                         "AND scheduled < '2026-01-01 12:00:00'")
        wait_for("subscriber concurrent partial expression index",
                 lambda: index_state(subscriber, "logical_expr") == [["t", "t", "t"]])
        expect_error(subscriber, overlapping, "23505")
        source_sql.query("DROP INDEX CONCURRENTLY logical_expr")
        wait_for("subscriber concurrent partial expression index removal",
                 lambda: index_state(subscriber, "logical_expr") == [])

        arm_phase(source, validated)
        builder.send(b"Q", b"CREATE UNIQUE INDEX CONCURRENTLY logical_b ON logical_rows(b)\0")
        wait_for_file(phase_file(source, validated, "paused"), source.process)
        writer.query("BEGIN")
        writer.query("UPDATE logical_rows SET v=101 WHERE a=1")
        phase_file(source, validated, "resume").touch()
        wait_for_fence()
        assert index_state(source_sql, "logical_b") == [["f", "t", "t"]]
        writer.query("COMMIT")
        finish_utility(builder, b"CREATE INDEX")
        source_sql.query("UPDATE logical_rows SET v=102 WHERE b=10")
        wait_for_query(subscriber, first_row, [["1", "10", "102"]])
        assert index_state(subscriber, "logical_b") == [["t", "t", "t"]]

        writer.query("BEGIN")
        writer.query("UPDATE logical_rows SET v=103 WHERE b=10")
        arm_phase(source, invalid)
        builder.send(b"Q", b"DROP INDEX CONCURRENTLY logical_b\0")
        wait_for_fence()
        assert index_state(source_sql, "logical_b") == [["t", "t", "t"]]
        writer.query("COMMIT")
        wait_for_file(phase_file(source, invalid, "paused"), source.process)
        assert index_state(source_sql, "logical_b") == [["f", "t", "t"]]
        source_sql.query("UPDATE logical_rows SET v=104 WHERE a=1")
        wait_for_query(subscriber, first_row, [["1", "10", "104"]])
        assert index_state(subscriber, "logical_b") == []
        phase_file(source, invalid, "resume").touch()
        finish_utility(builder, b"DROP INDEX")

        worker.terminate()
        worker.wait(timeout=10)
        progress = ("SELECT source_lsn FROM pg_lisp_subscription_progress "
                    "WHERE source_slot='index_phase' AND state='ready'")
        fault = source.root / "mixed-before-commit.marker"
        worker = start_worker("mixed-before-commit.log", {
            **os.environ,
            "PG_LISP_TEST_FAILPOINT": "before-target-commit",
            "PG_LISP_TEST_FAILPOINT_ACTION": "crash",
            "PG_LISP_TEST_FAILPOINT_ONCE_FILE": str(fault),
        })
        writer.query("BEGIN")
        writer.query("INSERT INTO logical_rows (a,b,v) VALUES (2,20,200)")
        writer.query("CREATE UNIQUE INDEX logical_mixed_ix ON logical_rows ((v + 1)) WHERE v > 100")
        writer.query("UPDATE logical_rows SET v=201 WHERE a=2")
        assert index_state(subscriber, "logical_mixed_ix") == []
        assert subscriber.query("SELECT v FROM logical_rows WHERE a=2") == []
        source_sql.query("CREATE TABLE logical_unrelated (id int)")
        writer.query("COMMIT")
        mixed_lsn = source_sql.query("SELECT pg_current_wal_flush_lsn()")[0][0]
        assert worker.wait(timeout=15) == -9
        assert fault.is_file()
        assert subscriber.query(f"SELECT source_lsn::pg_lsn < '{mixed_lsn}'::pg_lsn "
                                "FROM pg_lisp_subscription_progress "
                                "WHERE source_slot='index_phase' AND state='ready'") == [["t"]]
        assert index_state(subscriber, "logical_mixed_ix") == []
        assert subscriber.query("SELECT v FROM logical_rows WHERE a=2") == []
        assert subscriber.query("SELECT count(*) FROM logical_unrelated") == [["0"]]

        worker = start_worker("mixed-retry.log")
        wait_for_query(subscriber, "SELECT v FROM logical_rows WHERE a=2", [["201"]])
        assert index_state(subscriber, "logical_mixed_ix") == [["t", "t", "t"]]
        expect_error(subscriber, "INSERT INTO logical_rows (a,b,v) VALUES (3,30,201)", "23505")

        writer.query("BEGIN")
        writer.query("DROP INDEX logical_mixed_ix")
        writer.query("DELETE FROM logical_rows WHERE a=2")
        writer.query("ROLLBACK")
        source_sql.query("UPDATE logical_rows SET v=105 WHERE a=1")
        wait_for_query(subscriber, first_row, [["1", "10", "105"]])
        assert subscriber.query("SELECT v FROM logical_rows WHERE a=2") == [["201"]]
        assert index_state(subscriber, "logical_mixed_ix") == [["t", "t", "t"]]

        worker.terminate()
        worker.wait(timeout=10)
        fault = source.root / "mixed-after-commit.marker"
        worker = start_worker("mixed-after-commit.log", {
            **os.environ,
            "PG_LISP_TEST_FAILPOINT": "after-target-commit-before-feedback",
            "PG_LISP_TEST_FAILPOINT_ACTION": "crash",
            "PG_LISP_TEST_FAILPOINT_ONCE_FILE": str(fault),
        })
        writer.query("BEGIN")
        writer.query("DROP TABLE logical_rows")
        writer.query("CREATE TABLE logical_rows (a int primary key, b int, v int)")
        writer.query("INSERT INTO logical_rows VALUES (7,8,9)")
        writer.query("COMMIT")
        assert worker.wait(timeout=15) == -9
        assert fault.is_file()
        assert subscriber.query("SELECT a,b,v FROM logical_rows") == [["7", "8", "9"]]
        assert index_state(subscriber, "logical_mixed_ix") == []
        durable_marker = subscriber.query(progress)
        assert durable_marker
        subscriber.query("INSERT INTO logical_rows VALUES (8,9,10)")
        worker = start_worker("mixed-resume.log")
        source_sql.query("UPDATE logical_rows SET v=11 WHERE a=7")
        wait_for_query(subscriber, "SELECT a,b,v FROM logical_rows ORDER BY a",
                       [["7", "8", "11"], ["8", "9", "10"]])
        assert subscriber.query(progress) != durable_marker

        subscriber.query("CREATE INDEX subscriber_drift ON logical_rows (b)")
        writer.query("BEGIN")
        writer.query("CREATE INDEX logical_after_drift ON logical_rows (v)")
        writer.query("UPDATE logical_rows SET v=12 WHERE a=7")
        writer.query("COMMIT")
        drift_lsn = source_sql.query("SELECT pg_current_wal_flush_lsn()")[0][0]
        assert worker.wait(timeout=15) != 0
        assert "subscriber schema differs from publisher transaction contract" in (
            source.root / "mixed-resume.log"
        ).read_text()
        assert subscriber.query("SELECT v FROM logical_rows WHERE a=7") == [["11"]]
        assert index_state(subscriber, "logical_after_drift") == []
        assert subscriber.query(f"SELECT source_lsn::pg_lsn < '{drift_lsn}'::pg_lsn "
                                "FROM pg_lisp_subscription_progress "
                                "WHERE source_slot='index_phase' AND state='ready'") == [["t"]]
        subscriber.query("DROP INDEX subscriber_drift")
        worker = start_worker("mixed-drift-retry.log")
        wait_for_query(subscriber, "SELECT v FROM logical_rows WHERE a=7", [["12"]])
        assert index_state(subscriber, "logical_after_drift") == [["t", "t", "t"]]
        assert worker.poll() is None

        worker.terminate()
        worker.wait(timeout=10)
        target.control('(remhash "pg_lisp_transactional_ddl_supported" pg::*scalar-builtin-descriptors*)')
        worker = start_worker("incompatible-target.log")
        assert worker.wait(timeout=15) != 0
        assert "target cannot apply transactional DDL from the existing source slot" in (
            source.root / "incompatible-target.log"
        ).read_text()
        print("concurrent index logical replication: PASS")
    finally:
        for phase in (validated, invalid):
            release_phase(source, phase)
        for connection in connections:
            connection.close()
        if worker is not None and worker.poll() is None:
            worker.terminate()
            try:
                worker.wait(timeout=10)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=5)
        target.stop()
        source.stop()
        print(f"logical artifacts: {source.root}, {target.root}")


def logical_legacy_slot_fallback(legacy_endpoint):
    source, target = OwnedLisp(), OwnedLisp()
    worker = None
    try:
        source.start()
        target.start()
        if legacy_endpoint == "source":
            source.control("(setf (symbol-function 'pg::wal-native-slot-plugin-p) "
                           "(lambda (plugin) (equal plugin \"pg_lisp\")))")
        else:
            target.control('(remhash "pg_lisp_transactional_ddl_supported" pg::*scalar-builtin-descriptors*)')
        with contextlib.closing(Connection(source.port)) as writer, contextlib.closing(
            Connection(target.port)
        ) as subscriber, (source.root / "legacy-worker.log").open("wb") as log:
            writer.query("CREATE TABLE legacy_rows (id int primary key)")
            writer.query("INSERT INTO legacy_rows VALUES (1)")
            command = [str(LISP / "pg-lisp-subscribe"), "--slot", "legacy_worker",
                       "--source-port", str(source.port), "--target-port", str(target.port),
                       "--table", "legacy_rows"]
            worker = subprocess.Popen(command, cwd=LISP, stdout=log, stderr=subprocess.STDOUT)
            wait_for_query(subscriber, "SELECT id FROM legacy_rows", [["1"]])
            assert writer.query(
                "SELECT plugin FROM pg_replication_slots WHERE slot_name='legacy_worker'"
            ) == [["pg_lisp"]]
            writer.query("INSERT INTO legacy_rows VALUES (2)")
            wait_for_query(subscriber, "SELECT id FROM legacy_rows ORDER BY id", [["1"], ["2"]])
            worker.terminate()
            worker.wait(timeout=10)
            worker = subprocess.Popen(command, cwd=LISP, stdout=log, stderr=subprocess.STDOUT)
            writer.query("INSERT INTO legacy_rows VALUES (3)")
            wait_for_query(subscriber, "SELECT id FROM legacy_rows ORDER BY id", [["1"], ["2"], ["3"]])
            assert worker.poll() is None
    finally:
        if worker is not None and worker.poll() is None:
            worker.terminate()
            worker.wait(timeout=10)
        target.stop()
        source.stop()


def run_logical():
    logical_identity_cutover()
    logical_legacy_slot_fallback("source")
    logical_legacy_slot_fallback("target")


def expression_create_phases(server, builder, witness):
    unready = "after-concurrent-index-unready"
    ready = "after-concurrent-index-ready"
    witness.query("CREATE TABLE expression_build(id int, label text, active int)")
    witness.query("INSERT INTO expression_build VALUES (1, 'Alpha', 1), (2, 'ALPHA', 0)")
    arm_phase(server, unready)
    builder.send(b"Q", b"CREATE UNIQUE INDEX CONCURRENTLY expression_build_ix "
                 b"ON expression_build (lower(label)) WHERE active = 1\0")
    wait_for_file(phase_file(server, unready, "paused"), server.process)
    assert index_state(witness, "expression_build_ix") == [["f", "f", "t"]]
    witness.query("INSERT INTO expression_build VALUES (3, 'Beta', 1)")
    arm_phase(server, ready)
    release_phase(server, unready)
    wait_for_file(phase_file(server, ready, "paused"), server.process)
    assert index_state(witness, "expression_build_ix") == [["f", "t", "t"]]
    expect_error(witness, "UPDATE expression_build SET active = 1 WHERE id = 2", "23505")
    witness.query("UPDATE expression_build SET label = 'Gamma' WHERE id = 3")
    witness.query("INSERT INTO expression_build VALUES (4, 'Beta', 1)")
    release_phase(server, ready)
    finish_utility(builder, b"CREATE INDEX")
    assert index_state(witness, "expression_build_ix") == [["t", "t", "t"]]
    for label in ("ALPHA", "BETA", "GAMMA"):
        expect_error(witness, f"INSERT INTO expression_build VALUES (5, '{label}', 1)", "23505")
    witness.query("INSERT INTO expression_build VALUES (5, 'ALPHA', NULL)")


def expression_create_late_logical_interest(server, builder, witness):
    phase = "after-concurrent-index-validation"
    ordinary_build = "after-ordinary-index-build"
    witness.query("CREATE TABLE expression_guard (id int)")
    witness.query("INSERT INTO expression_guard VALUES (1)")
    database_oid = int(witness.query(
        "SELECT oid FROM pg_database WHERE datname = current_database()"
    )[0][0])
    arm_phase(server, phase)
    builder.send(b"Q", b"CREATE INDEX CONCURRENTLY expression_guard_ix "
                 b"ON expression_guard ((id + 1))\0")
    wait_for_file(phase_file(server, phase, "paused"), server.process)
    server.control(
        "(sb-thread:with-mutex (pg::*write-lock*) "
        "(pg::wal-register-logical-catalog-event-interest-under-writer-lock "
        f":concurrent-expression-test {database_oid}))"
    )
    try:
        mark_logical_catalog_wal(server)
        release_phase(server, phase)
        finish_utility(builder, b"CREATE INDEX")
        assert index_state(witness, "expression_guard_ix") == [["t", "t", "t"]]
        assert logical_catalog_records_since_mark(server) == 1
        mark_logical_catalog_wal(server)
        witness.query("CREATE INDEX CONCURRENTLY IF NOT EXISTS expression_guard_ix "
                      "ON expression_guard ((id + 1))")
        assert index_state(witness, "expression_guard_ix") == [["t", "t", "t"]]
        assert logical_catalog_records_since_mark(server) == 0

        arm_phase(server, ordinary_build)
        builder.send(b"Q", b"CREATE INDEX IF NOT EXISTS expression_name_race "
                     b"ON expression_guard ((id + 1))\0")
        wait_for_file(phase_file(server, ordinary_build, "paused"), server.process)
        witness.query("CREATE TABLE expression_name_race (id int)")
        mark_logical_catalog_wal(server)
        release_phase(server, ordinary_build)
        frames = utility_frames(builder)
        assert [kind for kind, _ in frames] == [b"N", b"C", b"Z"], frames
        assert b"C42P07\0" in frames[0][1], frames
        assert frames[1:] == [(b"C", b"CREATE INDEX\0"), (b"Z", b"I")], frames
        assert index_state(witness, "expression_name_race") == []
        assert logical_catalog_records_since_mark(server) == 0
    finally:
        release_phase(server, ordinary_build)
        server.control(
            "(pg::wal-release-logical-catalog-event-interest :concurrent-expression-test)"
        )
    witness.query("DROP INDEX CONCURRENTLY expression_guard_ix")
    witness.query("INSERT INTO expression_guard VALUES (2)")


def expression_build_fetches_only_demanded_toast(server, builder, witness):
    built = "after-concurrent-index-build"
    witness.query("CREATE TABLE demand_rows (id int, keep int, payload text)")
    witness.query("ALTER TABLE demand_rows ALTER COLUMN payload SET STORAGE EXTERNAL")
    payload = "".join(random.Random(19).choices("0123456789abcdefghijklmnopqrstuvwxyz", k=6400))
    values = ",".join(f"({i},{int(i == 0)},'{payload}')" for i in range(10))
    witness.query("INSERT INTO demand_rows VALUES " + values)
    server.control(
        "(progn (defparameter pg::*test-build-toast-fetches* 0) "
        "  (let ((fetch (symbol-function 'pg::toast-fetch-value))) "
        "    (defparameter pg::*test-original-toast-fetch* fetch) "
        "    (setf (symbol-function 'pg::toast-fetch-value) "
        "          (lambda (&rest args) "
        "            (incf pg::*test-build-toast-fetches*) (apply fetch args)))))"
    )

    def assert_fetches(expected):
        response = server.control('(format t "~&FETCHES=~d~%" pg::*test-build-toast-fetches*)')
        assert f"\nFETCHES={expected}\n" in response, response
        server.control("(setf pg::*test-build-toast-fetches* 0)")

    try:
        witness.query("CREATE INDEX demand_idx ON demand_rows ((length(payload) + id)) WHERE keep = 1")
        assert_fetches(1)
        assert_index_key_entries(server, "demand_idx", 6400, 1)
        assert_index_key_entries(server, "demand_idx", 6401, 0)
        witness.query("DROP INDEX demand_idx")
        witness.query("CREATE INDEX demand_predicate_idx ON demand_rows ((length(payload) + id)) "
                      "WHERE length(payload) = 6400 AND keep = 1")
        # Every row needs the predicate value. The winning row reuses it for its key.
        assert_fetches(10)
        assert_index_key_entries(server, "demand_predicate_idx", 6400, 1)
        assert_index_key_entries(server, "demand_predicate_idx", 6401, 0)
        witness.query("DROP INDEX demand_predicate_idx")
        arm_phase(server, built)
        builder.send(b"Q", b"CREATE INDEX CONCURRENTLY demand_idx "
                     b"ON demand_rows ((length(payload) + id)) WHERE keep = 1\0")
        wait_for_file(phase_file(server, built, "paused"), server.process)
        assert_fetches(1)
        witness.query(f"INSERT INTO demand_rows VALUES (10,1,'{payload}'),(11,0,'{payload}')")
        release_phase(server, built)
        finish_utility(builder, b"CREATE INDEX")
        assert_fetches(1)
        assert index_state(witness, "demand_idx") == [["t", "t", "t"]]
        assert_index_key_entries(server, "demand_idx", 6410, 1)
        assert_index_key_entries(server, "demand_idx", 6411, 0)
        witness.query("DROP TABLE demand_rows")
    finally:
        release_phase(server, built)
        server.control(
            "(progn (setf (symbol-function 'pg::toast-fetch-value) pg::*test-original-toast-fetch*) "
            "       (setf pg::*wal-test-pause-at* nil) "
            "       (makunbound 'pg::*test-original-toast-fetch*) "
            "       (makunbound 'pg::*test-build-toast-fetches*))"
        )


def partial_build_preserves_plain_key_compression(server, connection):
    connection.query("CREATE TABLE compressed_key_rows (id int, payload text)")
    fragment = "".join(random.Random(19).choices("abcdefghijklmnopqrstuvwxyz0123456789", k=2100))
    payload = fragment * 3
    connection.query(f"INSERT INTO compressed_key_rows VALUES (1,'{payload}')")
    response = server.control("""
      (let* ((pg::*backend* (pg::make-backend :database "postgres"))
             (table (pg::lookup-table "compressed_key_rows"))
             (tuple (pg::heapam-get-tuple (pg::pg-class-relfilenode table) 0 1 (pg::pg-class-columns table)))
             (value (aref (pg::htup-values tuple) 1)))
        (format t "~&EXTERNAL-COMPRESSED=~a~%"
                (and (pg::heap-external-varlena-p value)
                     (< (logand (pg::heap-external-varlena-external-info value) #x3fffffff)
                        (- (pg::heap-external-varlena-raw-size value) 4)))))""")
    assert "\nEXTERNAL-COMPRESSED=T\n" in response, response
    # Existing tuples keep compression, but the index build must inherit it.
    connection.query("ALTER TABLE compressed_key_rows ALTER COLUMN payload SET STORAGE EXTERNAL")
    for syntax in ("", "CONCURRENTLY "):
        connection.query(
            f"CREATE UNIQUE INDEX {syntax}compressed_key_idx ON compressed_key_rows (payload) "
            "WHERE length(payload) = 6300"
        )
        stored = server.control("""
          (let* ((pg::*backend* (pg::make-backend :database "postgres"))
                 (index (pg::lookup-index "compressed_key_idx"))
                 (node (pg::pg-index-relfilenode index)))
            (pg::with-btree-relation-lock (node)
              (let ((entries (pg::btree-prefix-entries-unlocked node #())))
                (format t "~&STORED-COMPRESSED=~a~%"
                        (and (= 1 (length entries)) (pg::btree-storage-key-p (caar entries)))))))""")
        assert "\nSTORED-COMPRESSED=T\n" in stored, stored
        expect_error(connection, f"INSERT INTO compressed_key_rows VALUES (2,'{payload}')", "23505")
        connection.query("DROP INDEX compressed_key_idx")
    connection.query("DROP TABLE compressed_key_rows")


def run():
    server = OwnedLisp()
    connections = []
    try:
        server.start()
        for _ in range(4):
            connections.append(Connection(server.port))
        drop_phases(server, *connections[:3])
        expression_create_phases(server, connections[0], connections[2])
        expression_create_late_logical_interest(server, connections[0], connections[2])
        create_phases(server, *connections[:3])
        unique_validation_failure(server, connections[0], connections[2])
        validation_accepts_replaced_versions(server, connections[0], connections[2])
        expression_build_fetches_only_demanded_toast(server, connections[0], connections[2])
        partial_build_preserves_plain_key_compression(server, connections[2])
        queued_vacuum_does_not_block_validation(server, *connections[:3])
        queued_read_committed_truncate_does_not_block_validation(server, *connections[:3])
        admitted_upsert_survives_invalidation(server, *connections)
        print("concurrent index DDL: PASS")
    finally:
        if (server.data / "pg_wal").exists():
            for marker in (server.data / "pg_wal").glob("test-paused-*"):
                server.data.joinpath("pg_wal", marker.name.replace("paused", "resume", 1)).touch()
        for connection in connections:
            connection.close()
        server.stop()
        print(f"artifacts: {server.root}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", nargs="?", default="sql",
                        choices=("sql", "crash", "physical", "logical"))
    suite = parser.parse_args().suite
    {"sql": run, "crash": crash_and_cancel, "physical": physical_index_phases,
     "logical": run_logical}[suite]()
