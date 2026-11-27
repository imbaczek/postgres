"""Verify transactional catalog visibility and recovery over the SQL wire protocol."""

import contextlib
from concurrent.futures import ThreadPoolExecutor
import runpy
import shutil
import struct
import subprocess
import time
from pathlib import Path


fixture = runpy.run_path(str(Path(__file__).with_name("command-complete.py")))
OwnedLisp, Connection = fixture["OwnedLisp"], fixture["Connection"]

from parking_wire import RawConnection, WireError
from binary_values import bind, cycle, frame, parse


def absent(connection, table):
    try:
        connection.query(f"SELECT id FROM {table}")
    except WireError as error:
        assert error.sqlstate == "42P01", error
    else:
        raise AssertionError(f"{table} is unexpectedly visible")


def verify_create(server):
    with contextlib.closing(Connection(server.port)) as writer, contextlib.closing(
        Connection(server.port)
    ) as observer:
        for table, outcome in (("abort_heap", "ROLLBACK"), ("commit_heap", "COMMIT")):
            writer.query("BEGIN")
            writer.query(f"CREATE TABLE {table} (id integer NOT NULL, value text)")
            writer.query(f"INSERT INTO {table} VALUES (42, 'private')")
            assert writer.query(f"SELECT id, value FROM {table}") == [["42", "private"]]
            absent(observer, table)
            writer.query(outcome)
        verify_committed(observer)


def verify_committed(connection):
    absent(connection, "abort_heap")
    absent(connection, "drop_heap")
    assert connection.query("SELECT id, value FROM commit_heap") == [["42", "private"]]


def verify_drop(server):
    with contextlib.closing(Connection(server.port)) as writer, contextlib.closing(
        Connection(server.port)
    ) as observer:
        writer.query("CREATE TABLE drop_heap (id integer)")
        writer.query("CREATE INDEX drop_heap_ix ON drop_heap(id)")
        writer.query("INSERT INTO drop_heap VALUES (1)")
        catalog = "SELECT relname FROM pg_class WHERE relname IN ('drop_heap', 'drop_heap_ix') ORDER BY relname"
        writer.query("BEGIN")
        writer.query("UPDATE drop_heap SET id = 2")
        writer.query("DROP TABLE drop_heap")
        assert writer.query(catalog) == []
        assert observer.query(catalog) == [["drop_heap"], ["drop_heap_ix"]]
        writer.query("ROLLBACK")
        assert observer.query("SELECT id FROM drop_heap") == [["1"]]

        writer.query("BEGIN")
        writer.query("UPDATE drop_heap SET id = 3")
        writer.query("DROP TABLE drop_heap")
        writer.query("COMMIT")
        absent(observer, "drop_heap")
        assert observer.query(catalog) == []


def verify_indexes(server):
    with contextlib.closing(Connection(server.port)) as writer, contextlib.closing(
        Connection(server.port)
    ) as observer:
        writer.query("CREATE TABLE index_heap (id integer NOT NULL)")
        writer.query("INSERT INTO index_heap VALUES (1)")
        catalog = "SELECT relname FROM pg_class WHERE relname IN ('discard_ix', 'keep_ix') ORDER BY relname"
        writer.query("BEGIN")
        writer.query("INSERT INTO index_heap VALUES (2)")
        writer.query("CREATE UNIQUE INDEX keep_ix ON index_heap(id)")
        writer.query("INSERT INTO index_heap VALUES (3)")
        writer.query("CREATE INDEX discard_ix ON index_heap(id)")
        writer.query("DROP INDEX discard_ix")
        assert writer.query(catalog) == [["keep_ix"]]
        assert observer.query(catalog) == []
        writer.query("COMMIT")
        assert observer.query(catalog) == [["keep_ix"]]

        writer.query("BEGIN")
        writer.query("DROP INDEX keep_ix")
        assert writer.query(catalog) == []
        assert observer.query(catalog) == [["keep_ix"]]
        writer.query("ROLLBACK")
        verify_committed_index(observer)


def verify_committed_index(connection):
    assert connection.query("SELECT id FROM index_heap ORDER BY id") == [["1"], ["2"], ["3"]]
    for value in (1, 2, 3):
        try:
            connection.query(f"INSERT INTO index_heap VALUES ({value})")
        except WireError as error:
            assert error.sqlstate == "23505", error
        else:
            raise AssertionError(f"committed unique index missed {value}")


def verify_constraint_indexes(server):
    with contextlib.closing(Connection(server.port)) as writer, contextlib.closing(
        Connection(server.port)
    ) as observer:
        writer.query("BEGIN")
        writer.query("CREATE TABLE constrained_heap (id integer PRIMARY KEY, "
                     "email text UNIQUE, tenant integer, slot integer, "
                     "CONSTRAINT tenant_slot UNIQUE (tenant, slot))")
        writer.query("INSERT INTO constrained_heap VALUES (1, 'example', 3, 4)")
        absent(observer, "constrained_heap")
        writer.query("COMMIT")
        verify_committed_constraints(observer)

        writer.query("BEGIN")
        writer.query("CREATE TABLE discarded_keys (id integer, PRIMARY KEY (id))")
        writer.query("INSERT INTO discarded_keys VALUES (1)")
        try:
            writer.query("INSERT INTO discarded_keys VALUES (1)")
        except WireError as error:
            assert error.sqlstate == "23505", error
        else:
            raise AssertionError("private primary key allowed a duplicate")
        writer.query("ROLLBACK")
        absent(observer, "discarded_keys")


def verify_committed_constraints(connection):
    assert connection.query("SELECT id FROM constrained_heap") == [["1"]]
    for values, state in (
        ("1, 'distinct', 5, 6", "23505"),
        ("2, 'example', 5, 6", "23505"),
        ("2, 'distinct', 3, 4", "23505"),
        ("NULL, 'distinct', 5, 6", "23502"),
    ):
        try:
            connection.query(f"INSERT INTO constrained_heap VALUES ({values})")
        except WireError as error:
            assert error.sqlstate == state, error
        else:
            raise AssertionError(f"constraint allowed ({values})")


def verify_implicit_catalog_block(connection):
    connection.query(
        "CREATE TABLE implicit_heap (id integer PRIMARY KEY); "
        "INSERT INTO implicit_heap VALUES (8); "
        "TRUNCATE implicit_heap; "
        "INSERT INTO implicit_heap VALUES (9); "
        "CREATE INDEX implicit_ix ON implicit_heap (id)"
    )
    assert connection.query("SELECT id FROM implicit_heap") == [["9"]]

    for sql, state in (
        ("CREATE TABLE implicit_abort (id integer PRIMARY KEY); "
         "INSERT INTO implicit_abort VALUES (1), (1)", "23505"),
        ("DROP INDEX implicit_ix; DROP TABLE implicit_heap; "
         "SELECT id FROM implicit_heap", "42P01"),
        ("TRUNCATE implicit_heap; INSERT INTO implicit_heap VALUES (1), (1)", "23505"),
    ):
        try:
            connection.query(sql)
        except WireError as error:
            assert error.sqlstate == state, error
        else:
            raise AssertionError("implicit catalog block succeeded")
    absent(connection, "implicit_abort")
    assert connection.query("SELECT id FROM implicit_heap") == [["9"]]
    assert connection.query("SELECT relname FROM pg_class WHERE relname = 'implicit_ix'") == [
        ["implicit_ix"]
    ]


def verify_expression_index(connection):
    connection.query("CREATE TABLE expression_heap (id integer, label text, active integer)")
    connection.query("INSERT INTO expression_heap VALUES (1, 'Alpha', 1), "
                     "(2, 'ALPHA', 0), (3, 'ALPHA', NULL)")
    connection.query("CREATE UNIQUE INDEX expression_ix ON expression_heap (lower(label)) WHERE active = 1")
    assert connection.query("SELECT indnkeyatts, indkey FROM pg_index "
                            "WHERE indexrelid = (SELECT oid FROM pg_class "
                            "WHERE relname = 'expression_ix')") == [["1", "0"]]
    assert connection.query("SELECT id FROM expression_heap WHERE lower(label) = 'alpha' "
                            "ORDER BY id") == [["1"], ["2"], ["3"]]
    assert connection.query("SELECT id FROM expression_heap ORDER BY id LIMIT 1") == [["1"]]
    for sql, state in (
        ("INSERT INTO expression_heap VALUES (4, 'alpha', 1)", "23505"),
        ("UPDATE expression_heap SET active = 1 WHERE id = 2", "23505"),
    ):
        try:
            connection.query(sql)
        except WireError as error:
            assert error.sqlstate == state, error
        else:
            raise AssertionError(f"partial unique index accepted {sql!r}")
    connection.query("INSERT INTO expression_heap VALUES (4, 'alpha', 1) ON CONFLICT DO NOTHING")
    connection.query("UPDATE expression_heap SET active = 0 WHERE id = 1")
    connection.query("UPDATE expression_heap SET active = 1 WHERE id = 2")
    connection.query("UPDATE expression_heap SET label = 'Beta' WHERE id = 2")
    connection.query("UPDATE expression_heap SET active = 1 WHERE id = 3")
    connection.query("CREATE UNIQUE INDEX expression_partial_plain ON expression_heap (id) WHERE active = 1")
    try:
        connection.query("INSERT INTO expression_heap VALUES (6, 'Other', 0) ON CONFLICT (id) DO NOTHING")
    except WireError as error:
        assert error.sqlstate == "42P10", error
    else:
        raise AssertionError("a partial index was inferred for an unconditional target")
    connection.query("CREATE UNIQUE INDEX expression_parenthesized_plain ON expression_heap ((id))")
    connection.query("INSERT INTO expression_heap VALUES (2, 'Other', 0) ON CONFLICT (id) DO NOTHING")
    assert connection.query("SELECT label FROM expression_heap WHERE id = 2") == [["Beta"]]
    connection.query("BEGIN")
    connection.query("CREATE INDEX expression_rollback ON expression_heap ((id + 1)) WHERE active = 1")
    connection.query("ROLLBACK")
    connection.query("CREATE TABLE numeric_index_heap (id integer, amount numeric)")
    connection.query("INSERT INTO numeric_index_heap VALUES (7, 1.2)")
    connection.query("CREATE UNIQUE INDEX numeric_index_ix ON numeric_index_heap (id) "
                     "WHERE amount = ANY ('{1.2,3.4}')")
    verify_restored_expression_index(connection)


def verify_restored_expression_index(connection):
    assert connection.query("SELECT id FROM expression_heap WHERE active = 1 ORDER BY id") == [["2"], ["3"]]
    assert connection.query("SELECT relname FROM pg_class WHERE relname = 'expression_rollback'") == []
    try:
        connection.query("INSERT INTO expression_heap VALUES (5, 'BETA', 1)")
    except WireError as error:
        assert error.sqlstate == "23505", error
    else:
        raise AssertionError("expression index missed the updated key")
    try:
        connection.query("INSERT INTO numeric_index_heap VALUES (7, 3.4)")
    except WireError as error:
        assert error.sqlstate == "23505", error
    else:
        raise AssertionError("numeric-array predicate missed a conflicting row")


def verify_expression_index_plan(connection):
    connection.query("CREATE TABLE expression_plan (id integer, label text, active integer)")
    for start in range(0, 1200, 200):
        values = ", ".join(
            f"({row}, 'Label{row % 600}', {int(row < 600)})"
            for row in range(start, start + 200)
        )
        connection.query("INSERT INTO expression_plan VALUES " + values)
    connection.query(
        "CREATE UNIQUE INDEX expression_plan_ix ON expression_plan (lower(label)) "
        "WHERE active > 0 AND id IS NOT NULL"
    )
    connection.query("ANALYZE expression_plan")
    probe = "SELECT id FROM expression_plan WHERE lower(label) = 'label7'"
    for qualifying in (probe + " AND active > 0 AND id IS NOT NULL",
                       probe + " AND active >= 1 AND id >= 0",
                       probe + " AND active = 1 AND id >= 0"):
        assert "Index Scan using expression_plan_ix" in connection.query("EXPLAIN " + qualifying)[0][0]
        assert connection.query(qualifying) == [["7"]]
    for query in (probe, probe + " AND (active = 0 OR active = 1)",
                  probe + " AND active >= 0 AND id >= 0"):
        assert "expression_plan_ix" not in connection.query("EXPLAIN " + query)[0][0]
        assert sorted(connection.query(query)) == [["607"], ["7"]]
    statement = b"expression_parameter"
    parsed = cycle(connection, [
        parse("SELECT id FROM expression_plan WHERE lower(label) = $1 "
              "AND active = $2 AND id >= 0", (25, 23), name=statement)
    ], with_message=True)
    assert parsed == [], parsed
    for parameter, expected in ((b"1", [7]), (b"0", [607]), (None, [])):
        messages = cycle(connection, [
            bind((b"label7", parameter), statement=statement), frame(b"E", bytes(5))
        ])
        assert all(message[0] != b"E" for message in messages), messages
        assert [int(body[6:]) for kind, body in messages if kind == b"D"] == expected
    connection.query("DROP INDEX expression_plan_ix")
    connection.query(
        "CREATE UNIQUE INDEX expression_composite_ix ON expression_plan (active, lower(label)) "
        "WHERE id IS NOT NULL"
    )
    query = probe + " AND active = 1 AND id >= 0"
    assert "Index Scan using expression_composite_ix" in connection.query("EXPLAIN " + query)[0][0]
    assert connection.query(query) == [["7"]]


def crash_during_statement(server, connection, point, statement):
    marker = server.data / f"pg_wal/test-paused-{point}"
    marker.unlink(missing_ok=True)
    server.control(f'(setf pg::*wal-test-pause-at* "{point}")')
    connection.send(b"Q", statement.encode() + b"\0")
    fixture["wait_for_file"](marker, server.process)
    server.stop(immediate=True)
    marker.unlink()


def verify_crash_boundary(server, point, table, committed):
    with contextlib.closing(Connection(server.port)) as writer:
        writer.query("BEGIN")
        writer.query(f"CREATE TABLE {table} (id integer)")
        writer.query(f"INSERT INTO {table} VALUES (7)")
        crash_during_statement(server, writer, point, "COMMIT")
    server.start()
    with contextlib.closing(Connection(server.port)) as reader:
        if committed:
            assert reader.query(f"SELECT id FROM {table}") == [["7"]]
        else:
            absent(reader, table)


def verify_expression_index_crash_boundaries(server):
    ddl = "CREATE UNIQUE INDEX crash_expression_ix ON crash_expression (lower(label)) WHERE active = 1"
    with contextlib.closing(Connection(server.port)) as writer:
        writer.query("CREATE TABLE crash_expression (id int, label text, active int)")
        writer.query("INSERT INTO crash_expression VALUES (1,'Alpha',1),(2,'Alpha',0)")
        crash_during_statement(server, writer, "after-ordinary-index-build", ddl)
    server.start()

    with contextlib.closing(Connection(server.port)) as writer:
        assert writer.query("SELECT relname FROM pg_class WHERE relname='crash_expression_ix'") == []
        assert writer.query("SELECT id, active FROM crash_expression ORDER BY id") == [["1", "1"], ["2", "0"]]
        writer.query("BEGIN")
        writer.query(ddl)
        writer.query("UPDATE crash_expression SET label='Beta',active=1 WHERE id=2")
        crash_during_statement(server, writer, "after-catalog-transaction-wal-flush", "COMMIT")
    server.start()

    def verify_values(connection, label):
        assert connection.query(
            "SELECT indisvalid, indisunique FROM pg_index WHERE indexrelid = "
            "(SELECT oid FROM pg_class WHERE relname='crash_expression_ix')"
        ) == [["t", "t"]]
        assert connection.query("SELECT id, label FROM crash_expression ORDER BY id") == [["1", "Alpha"], ["2", label]]
        assert connection.query(
            f"BEGIN; INSERT INTO crash_expression VALUES (3,'{label.upper()}',1) "
            "ON CONFLICT DO NOTHING RETURNING id; ROLLBACK"
        ) == []

    for point, before, after in (
        ("after-page-images", "Beta", "Beta"),
        ("after-wal-flush", "Beta", "Gamma"),
    ):
        with contextlib.closing(Connection(server.port)) as writer:
            verify_values(writer, before)
            crash_during_statement(server, writer, point, "UPDATE crash_expression SET label='Gamma' WHERE id=2")
        server.start()
        with contextlib.closing(Connection(server.port)) as reader:
            verify_values(reader, after)
            free_key = "GAMMA" if after == "Beta" else "BETA"
            assert reader.query(
                f"BEGIN; INSERT INTO crash_expression VALUES (3,'{free_key}',1) "
                "ON CONFLICT DO NOTHING RETURNING id; ROLLBACK"
            ) == [["3"]]


def verify_expression_index_rebind(server, connection):
    statement = b"expression_invalidation"
    query = ("SELECT id FROM expression_plan WHERE lower(label) = $1 "
             "AND active = 1 AND id >= 0")
    assert cycle(connection, [parse(query, (25,), name=statement)], with_message=True) == []

    def execute(value):
        messages = cycle(connection, [
            bind((value,), statement=statement), frame(b"E", bytes(5))
        ], with_message=True)
        assert all(message[0] != b"E" for message in messages), messages
        return [int(body[6:]) for kind, body in messages if kind == b"D"]

    assert execute(b"label7") == [7]
    assert execute(None) == []
    connection.socket.sendall(
        bind((b"label7",), statement=statement) + frame(b"E", b"\0\0\0\0\1") + frame(b"H", b"")
    )
    while True:
        kind, body = connection.receive()
        assert kind != b"E", body
        if kind == b"s":
            break
    with contextlib.closing(Connection(server.port)) as observer:
        observer.query("SET statement_timeout = '100ms'")
        observer.query("BEGIN")
        try:
            observer.query("DROP INDEX expression_composite_ix")
        except WireError as error:
            assert error.sqlstate == "57014", error
        else:
            raise AssertionError("DROP INDEX bypassed a suspended expression-index scan")
        observer.query("ROLLBACK")
        observer.query("RESET statement_timeout")
        connection.send(b"C", b"P\0")
        connection.send(b"S", b"")
        assert connection.receive()[0] == b"3"
        assert connection.receive()[0] == b"Z"
        observer.query("BEGIN")
        observer.query("DROP INDEX expression_composite_ix")
        observer.query(
            "CREATE UNIQUE INDEX expression_composite_ix ON expression_plan (active, lower(label)) "
            "WHERE active = 0"
        )
        observer.query("COMMIT")
    assert execute(b"label7") == [7]
    assert execute(b"label9") == [9]


def verify_partial_range_index(connection):
    connection.query("CREATE TABLE partial_ranges (active integer, id integer)")
    for start in range(0, 1200, 200):
        rows = ",".join(f"({int(row % 100 == 0)}, {row})" for row in range(start, start + 200))
        connection.query("INSERT INTO partial_ranges VALUES " + rows)
    connection.query("ANALYZE partial_ranges")
    connection.query(
        "CREATE INDEX expression_partial_range ON partial_ranges (active, id) WHERE active > 0"
    )
    base = "SELECT id FROM partial_ranges WHERE active = 1 AND id >= 790 AND id < 1100"
    ordered = base + " ORDER BY id DESC LIMIT 3"
    assert "Index Scan using expression_partial_range" in connection.query("EXPLAIN " + base)[0][0]
    plan = connection.query("EXPLAIN " + ordered)[0][0]
    assert plan.startswith("Limit") and "Index Scan using expression_partial_range" in plan, plan
    assert connection.query(ordered) == [["1000"], ["900"], ["800"]]
    excluded = ("SELECT id FROM partial_ranges WHERE active = 0 AND id >= 790 AND id < 1100 "
                "ORDER BY id DESC LIMIT 3")
    assert "expression_partial_range" not in connection.query("EXPLAIN " + excluded)[0][0]
    assert connection.query(excluded) == [["1099"], ["1098"], ["1097"]]


def verify_new_logical_interest(server):
    with contextlib.closing(Connection(server.port)) as writer, contextlib.closing(
        Connection(server.port)
    ) as observer:
        writer.query("BEGIN")
        writer.query("CREATE TABLE logical_guard_heap (id integer)")
        database_oid = int(writer.query(
            "SELECT oid FROM pg_database WHERE datname = current_database()"
        )[0][0])
        registration = (
            f"(pg::wal-register-logical-catalog-event-interest-under-writer-lock "
            f":test-transactional-catalog {database_oid})"
        )
        server.control(f"(sb-thread:with-mutex (pg::*write-lock*) {registration})")
        try:
            try:
                writer.query("COMMIT")
            except WireError as error:
                assert error.sqlstate == "0A000", error
            else:
                raise AssertionError("catalog commit ignored new logical capture interest")
            absent(observer, "logical_guard_heap")
        finally:
            server.control(
                "(pg::wal-release-logical-catalog-event-interest :test-transactional-catalog)"
            )


def verify_catalog_capture_records(server):
    result = server.control("""(pg::call-with-logical-replication-slot "catalog_capture"
      (lambda (slot)
        (declare (ignore slot))
        (pg::call-with-logical-reader-spools
         (lambda ()
           (flet ((operation-kind (change)
                    (let ((kind (pg::logical-change-kind change)))
                      (if (eq kind :transaction-ddl)
                          (or (getf (pg::logical-decode-transaction-ddl
                                 (pg::logical-change-event-payload change)
                                 (pg::logical-change-database-oid change)) :type)
                              :schema-contract)
                          kind))))
             (format t "~&CATALOG-CAPTURE=~s~%"
               (loop for transaction in (pg::wal-logical-slot-read-transactions "catalog_capture")
                     collect (let ((kinds nil))
                               (pg::logical-transaction-map-changes
                                transaction (lambda (change) (push (operation-kind change) kinds)))
                               (nreverse kinds)))))))))""")
    expected = ("CATALOG-CAPTURE=((:CREATE-TABLE :INSERT :CREATE-INDEX :UPDATE :SCHEMA-CONTRACT)"
                " (:CREATE-TABLE :INSERT :DROP-TABLE :SCHEMA-CONTRACT)"
                " (:DROP-INDEX :DELETE :SCHEMA-CONTRACT))")
    assert expected in " ".join(result.split()), result


def verify_first_slot_activation_during_ddl(server):
    with contextlib.closing(Connection(server.port)) as writer, contextlib.closing(
        Connection(server.port)
    ) as activation:
        writer.query("BEGIN")
        writer.query("CREATE TABLE first_slot_pending (id int)")
        activation.send(b"Q", b"SELECT pg_create_logical_replication_slot('first_slot','pgoutput')\0")
        for _ in range(100):
            if "\nT\n" in server.control(
                "(sb-thread:with-mutex (pg::*wal-lock*) (pg::wal-logical-capture-required-under-lock-p))"
            ):
                break
            time.sleep(0.02)
        else:
            raise AssertionError("slot creation did not announce logical capture")
        try:
            writer.query("COMMIT")
        except WireError as error:
            assert error.sqlstate == "0A000", error
        else:
            raise AssertionError("publisher committed uncaptured DDL during slot activation")
        writer.query("ROLLBACK")
        while True:
            kind, payload = activation.receive()
            assert kind != b"E", payload
            if kind == b"Z":
                break
        absent(activation, "first_slot_pending")
        activation.query("SELECT pg_drop_replication_slot('first_slot')")


def verify_net_zero_ddl_slot_admission(server):
    with contextlib.closing(Connection(server.port)) as writer, contextlib.closing(
        Connection(server.port)
    ) as activation:
        [[database_oid]] = writer.query("SELECT oid FROM pg_database WHERE datname=current_database()")
        writer.query("CREATE TABLE net_zero_committed (id int)")
        writer.query("BEGIN")
        writer.query("CREATE TABLE net_zero_transient (id int)")
        writer.query("INSERT INTO net_zero_transient VALUES (2)")
        writer.query("DROP TABLE net_zero_transient")
        writer.query("INSERT INTO net_zero_committed VALUES (2)")
        writer.query("COMMIT")
        assert activation.query("SELECT id FROM net_zero_committed") == [["2"]]
        writer.query("BEGIN")
        writer.query("INSERT INTO net_zero_committed VALUES (1)")
        writer.query("CREATE TABLE net_zero_transient (id int)")
        writer.query("INSERT INTO net_zero_transient VALUES (1)")
        writer.query("DROP TABLE net_zero_transient")
        activation.send(b"Q", b"SELECT pg_create_logical_replication_slot('net_zero_slot','pgoutput')\0")
        deadline = time.monotonic() + 10
        while ":INCOMPATIBLE" not in server.control(
            f"(sb-thread:with-mutex (pg::*wal-lock*) (pg::wal-transactional-ddl-mode-under-lock {database_oid}))"
        ):
            assert time.monotonic() < deadline, "slot creation did not publish capture interest"
            time.sleep(0.01)
        try:
            writer.query("COMMIT")
        except WireError as error:
            assert error.sqlstate == "0A000" and "compatible logical replication slots" in str(error), error
        else:
            raise AssertionError("net-zero DDL bypassed the replication-slot check")
        writer.query("ROLLBACK")
        while True:
            kind, payload = activation.receive()
            assert kind != b"E", payload
            if kind == b"Z":
                break
        assert activation.query("SELECT id FROM net_zero_committed") == [["2"]]
        absent(activation, "net_zero_transient")
        activation.query("SELECT pg_drop_replication_slot('net_zero_slot')")
        activation.query("DROP TABLE net_zero_committed")


def expect_slot_capability_error(client, start, message, sqlstate="0A000"):
    client.socket.sendall(frame(b"Q", (start + "\0").encode()))
    kind, body = client.receive()
    while kind == b"N":
        kind, body = client.receive()
    assert kind == b"E", (kind, body)
    error = WireError(client.error_fields(body))
    assert error.sqlstate == sqlstate and message in str(error), error
    assert client.receive()[0] == b"Z"


def verify_transactional_slot_handshake(server):
    with contextlib.closing(RawConnection(server.port, replication="database")) as client:
        _, legacy_birth, _, _ = client.query("CREATE_REPLICATION_SLOT legacy_slot LOGICAL pg_lisp")[0]
        expect_slot_capability_error(
            client, f"START_REPLICATION SLOT legacy_slot LOGICAL {legacy_birth} (transactional_ddl 'true')",
            "does not support transactional DDL",
        )
        client.query("DROP_REPLICATION_SLOT legacy_slot")
    return create_confirmed_transactional_slot(server, "transactional_slot", binary=True)


def create_confirmed_transactional_slot(server, name, binary=False):
    with contextlib.closing(RawConnection(server.port, replication="database")) as client:
        create_options = " (binary_bytea true)" if binary else ""
        _, birth, _, plugin = client.query(
            f"CREATE_REPLICATION_SLOT {name} LOGICAL pg_lisp_transactional{create_options}"
        )[0]
        assert plugin == "pg_lisp_transactional"
        start = f"START_REPLICATION SLOT {name} LOGICAL {birth}"
        if binary:
            expect_slot_capability_error(client, start, "requires transactional DDL")
            expect_slot_capability_error(client, start + " (transactional_ddl 'false')", "requires transactional DDL")
            expect_slot_capability_error(
                client, start + " (transactional_ddl 'true')", "requires binary bytea",
            )
        binary_option = ", binary_bytea 'true'" if binary else ""
        client.socket.sendall(frame(
            b"Q", (start + f" (transactional_ddl 'true'{binary_option})\0").encode()
        ))
        assert client.receive()[0] == b"W"
        high, low = (int(word, 16) for word in birth.split("/"))
        position = (high << 32) | low
        client.socket.sendall(frame(b"d", b"r" + struct.pack("!QQQQB", position, position, position, 0, 0)))
        with contextlib.closing(Connection(server.port)) as observer:
            for _ in range(100):
                if observer.query(
                    "SELECT plugin,confirmed_flush_lsn FROM pg_replication_slots "
                    f"WHERE slot_name='{name}'"
                ) == [[plugin, birth]]:
                    return birth
                time.sleep(0.02)
        raise AssertionError("transactional slot did not confirm the initial copy")


def verify_transactional_slot_resume(server, birth):
    with contextlib.closing(RawConnection(server.port, replication="database")) as client:
        start = f"START_REPLICATION SLOT transactional_slot LOGICAL {birth}"
        expect_slot_capability_error(client, start, "requires transactional DDL")
        client.socket.sendall(frame(
            b"Q", (start + " (transactional_ddl 'true', binary_bytea 'true')\0").encode()
        ))
        assert client.receive()[0] == b"W"
    with contextlib.closing(RawConnection(server.port, replication="database")) as client:
        client.query("DROP_REPLICATION_SLOT transactional_slot WAIT")


def verify_apply_requires_schema_contract(server):
    with contextlib.closing(fixture["QueryConnection"](server.port, "pg-lisp-subscribe-apply")) as target:
        target.query("CREATE TABLE apply_requires_contract (id int)")
        target.query("BEGIN")
        target.query(
            "APPLY LOGICAL EVENT 'transaction-ddl' 1 "
            """'(:version 1 :database-oid 7 :ast (:type :create-index :name "missing_contract_ix"
                 :table "apply_requires_contract" :columns ("id")))'"""
        )
        target.query("INSERT INTO apply_requires_contract VALUES (1)")
        try:
            target.query("COMMIT")
        except WireError as error:
            assert error.sqlstate == "08P01" and "missing transactional" in str(error), error
        else:
            raise AssertionError("mixed apply committed without a schema contract")
    with contextlib.closing(Connection(server.port)) as observer:
        assert observer.query("SELECT id FROM apply_requires_contract") == []
        assert observer.query("SELECT oid FROM pg_class WHERE relname='missing_contract_ix'") == []
        observer.query("DROP TABLE apply_requires_contract")


def verify_commit_dependency_races(server):
    with contextlib.closing(Connection(server.port)) as writer, contextlib.closing(
        Connection(server.port)
    ) as concurrent:
        def expect_conflict(text):
            try:
                writer.query("COMMIT")
            except WireError as error:
                assert error.sqlstate == "40001" and text in str(error), error
                writer.query("ROLLBACK")
            else:
                raise AssertionError("catalog commit ignored a concurrent dependency change")

        concurrent.query("CREATE TABLE publication_race (id int)")
        concurrent.query("INSERT INTO publication_race VALUES (7)")
        writer.query("BEGIN")
        writer.query("DROP TABLE publication_race")
        writer.query("CREATE TABLE publication_race (id int)")
        concurrent.query("CREATE PUBLICATION race_pub FOR TABLE publication_race")
        expect_conflict("publication membership")
        assert concurrent.query("SELECT id FROM publication_race") == [["7"]]
        concurrent.query("DROP PUBLICATION race_pub")
        concurrent.query("CREATE PUBLICATION race_all_pub FOR ALL TABLES")
        writer.query("BEGIN")
        writer.query("DROP TABLE publication_race")
        writer.query("COMMIT")
        absent(concurrent, "publication_race")
        concurrent.query("DROP PUBLICATION race_all_pub")

        concurrent.query("CREATE TABLE publication_column_race (id int, flag boolean)")
        writer.query("BEGIN")
        writer.query("ALTER TABLE publication_column_race DROP COLUMN flag")
        concurrent.query(
            "CREATE PUBLICATION column_race_pub FOR TABLE publication_column_race WHERE (flag)"
        )
        expect_conflict("publication filter")
        assert concurrent.query("SELECT flag FROM publication_column_race") == []
        writer.query("BEGIN")
        writer.query("ALTER TABLE publication_column_race DROP COLUMN flag CASCADE")
        concurrent.query("ALTER PUBLICATION column_race_pub SET (publish = 'insert')")
        expect_conflict("publication column_race_pub")
        assert concurrent.query("SELECT flag FROM publication_column_race") == []
        concurrent.query("DROP PUBLICATION column_race_pub")
        concurrent.query("DROP TABLE publication_column_race")

        concurrent.query("CREATE SEQUENCE sequence_race")
        writer.query("BEGIN")
        writer.query("CREATE TABLE sequence_race_table (id bigint DEFAULT nextval('sequence_race'))")
        concurrent.query("DROP SEQUENCE sequence_race")
        concurrent.query("CREATE SEQUENCE sequence_race START WITH 30")
        expect_conflict("sequence_race")
        absent(concurrent, "sequence_race_table")

        writer.query("BEGIN")
        writer.query("CREATE TABLE sequence_race_table (id bigint DEFAULT nextval('sequence_race'))")
        concurrent.query("ALTER SEQUENCE sequence_race RESTART WITH 40")
        writer.query("COMMIT")
        assert writer.query("INSERT INTO sequence_race_table DEFAULT VALUES RETURNING id") == [["40"]]
        concurrent.query("DROP TABLE sequence_race_table")

        create_confirmed_transactional_slot(server, "transient_dependency")
        try:
            writer.query("BEGIN")
            writer.query("CREATE TABLE sequence_race_table (id bigint DEFAULT nextval('sequence_race'))")
            concurrent.query("DROP SEQUENCE sequence_race")
            writer.query("DROP TABLE sequence_race_table")
            expect_conflict("sequence_race")
            absent(concurrent, "sequence_race_table")
        finally:
            concurrent.query("SELECT pg_drop_replication_slot('transient_dependency')")


def verify_legacy_slot_noops(server):
    with contextlib.closing(Connection(server.port)) as writer, contextlib.closing(
        Connection(server.port)
    ) as concurrent:
        writer.query("CREATE TABLE legacy_noop (id int)")
        writer.query("CREATE INDEX legacy_noop_index ON legacy_noop(id)")
        writer.query("SELECT pg_create_logical_replication_slot('mixed_legacy','pgoutput')")
        standard_birth = writer.query(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='mixed_legacy'"
        )[0][0]
        with contextlib.closing(RawConnection(server.port, replication="database")) as standard_client:
            expect_slot_capability_error(
                standard_client, f"START_REPLICATION SLOT mixed_legacy LOGICAL {standard_birth} "
                "(proto_version '1', publication_names 'test', transactional_ddl 'true')",
                "private replication option", sqlstate="42601",
            )
        writer.query("BEGIN")
        writer.query("CREATE TABLE IF NOT EXISTS legacy_noop (id int)")
        writer.query("CREATE INDEX IF NOT EXISTS legacy_noop_index ON legacy_noop(id)")
        writer.query("DROP TABLE IF EXISTS legacy_noop_missing")
        writer.query("DROP INDEX IF EXISTS legacy_noop_missing")
        writer.query("INSERT INTO legacy_noop VALUES (1)")
        writer.query("COMMIT")
        assert concurrent.query("SELECT id FROM legacy_noop") == [["1"]]
        for statement in (
            "CREATE TABLE unsupported_capture (id int)",
            "CREATE INDEX unsupported_capture_ix ON legacy_noop(id)",
            "DROP TABLE IF EXISTS legacy_noop_missing, legacy_noop",
            "DROP INDEX IF EXISTS legacy_noop_missing, legacy_noop_index",
            "ALTER TABLE legacy_noop ALTER COLUMN id TYPE bigint",
        ):
            writer.query("BEGIN")
            try:
                writer.query(statement)
            except WireError as error:
                assert error.sqlstate == "0A000" and "compatible logical replication slots" in str(error), error
            else:
                raise AssertionError(f"publisher admitted {statement} with a standard slot")
            writer.query("ROLLBACK")
        assert concurrent.query("SELECT id FROM legacy_noop") == [["1"]]
        assert concurrent.query("SELECT relname FROM pg_class WHERE relname='legacy_noop_index'") == [
            ["legacy_noop_index"]
        ]
        writer.query("SELECT pg_drop_replication_slot('mixed_legacy')")
        writer.query("DROP TABLE legacy_noop")


def verify_ordered_catalog_capture(server):
    create_confirmed_transactional_slot(server, "catalog_capture")
    verify_new_logical_interest(server)
    with contextlib.closing(Connection(server.port)) as writer, contextlib.closing(
        Connection(server.port)
    ) as concurrent:
        writer.query("BEGIN")
        writer.query("CREATE TABLE late_legacy_capture (id int)")
        concurrent.query("SELECT pg_create_logical_replication_slot('late_legacy','pgoutput')")
        try:
            writer.query("COMMIT")
        except WireError as error:
            assert error.sqlstate == "0A000" and "compatible logical replication slots" in str(error), error
        else:
            raise AssertionError("publisher committed captured DDL after a standard slot appeared")
        absent(concurrent, "late_legacy_capture")
        writer.query("ROLLBACK")
        concurrent.query("SELECT pg_drop_replication_slot('late_legacy')")

        writer.query("BEGIN")
        writer.query("CREATE TABLE capture_items (id int primary key)")
        writer.query("CREATE TABLE IF NOT EXISTS capture_items (id int)")
        writer.query("INSERT INTO capture_items VALUES (42)")
        writer.query("CREATE UNIQUE INDEX capture_index ON capture_items ((id + 1)) WHERE id > 0")
        writer.query("UPDATE capture_items SET id = 43 WHERE id = 42")
        writer.query("DROP INDEX IF EXISTS capture_missing")
        writer.query("COMMIT")

        writer.query("BEGIN")
        writer.query("CREATE TABLE capture_transient (id int)")
        writer.query("INSERT INTO capture_transient VALUES (1)")
        writer.query("DROP TABLE capture_transient")
        writer.query("COMMIT")
        absent(writer, "capture_transient")

        writer.query("BEGIN")
        writer.query("DROP INDEX capture_index")
        writer.query("DELETE FROM capture_items WHERE id = 43")
        writer.query("ROLLBACK")
        assert writer.query("SELECT id FROM capture_items") == [["43"]]

        writer.query("BEGIN")
        writer.query("DROP INDEX capture_index")
        writer.query("DELETE FROM capture_items WHERE id = 43")
        writer.query("COMMIT")
        assert writer.query("SELECT id FROM capture_items") == []

        verify_catalog_capture_records(server)

def verify_column_addition(connection):
    connection.query("CREATE TABLE evolved_stats (" + ", ".join(f"c{i} integer" for i in range(8)) + ")")
    connection.query("INSERT INTO evolved_stats VALUES (" + ", ".join(["NULL"] * 8) + ")")
    connection.query("ALTER TABLE evolved_stats ADD COLUMN payload json")
    connection.query("ANALYZE evolved_stats")
    assert connection.query("SELECT payload FROM evolved_stats") == [[None]]
    connection.query("CREATE TABLE evolved_heap (id integer PRIMARY KEY)")
    connection.query("INSERT INTO evolved_heap VALUES (1)")
    connection.query("ALTER TABLE evolved_heap ADD COLUMN flag boolean")
    assert connection.query("SELECT * FROM evolved_heap") == [["1", None]]

    connection.query("BEGIN")
    connection.query("ALTER TABLE evolved_heap ADD COLUMN abandoned integer")
    connection.query("INSERT INTO evolved_heap VALUES (2, false, 42)")
    connection.query("ROLLBACK")
    assert connection.query("SELECT * FROM evolved_heap") == [["1", None]]

    connection.query("BEGIN")
    connection.query("ALTER TABLE evolved_heap ADD COLUMN label text")
    connection.query("INSERT INTO evolved_heap VALUES (3, false, 'committed')")
    connection.query("COMMIT")
    connection.query("ALTER TABLE evolved_heap ADD COLUMN IF NOT EXISTS label text")
    connection.query("CREATE INDEX evolved_label_idx ON evolved_heap(label)")
    connection.query("VACUUM FULL evolved_heap")

    connection.query("CREATE TABLE evolved_defaults (id integer PRIMARY KEY)")
    connection.query("INSERT INTO evolved_defaults VALUES (1)")
    connection.query("ALTER TABLE evolved_defaults ADD COLUMN flag boolean DEFAULT false")
    connection.query("INSERT INTO evolved_defaults VALUES (2, NULL)")
    connection.query("ALTER TABLE evolved_defaults ALTER COLUMN flag SET DEFAULT true")
    connection.query("INSERT INTO evolved_defaults (id) VALUES (3)")
    connection.query("ALTER TABLE evolved_defaults ALTER COLUMN flag DROP DEFAULT")
    connection.query("INSERT INTO evolved_defaults (id) VALUES (4)")

    connection.query(
        "CREATE TABLE evolved_rename (id integer PRIMARY KEY, label text CHECK (label <> 'forbidden'))"
    )
    connection.query("CREATE INDEX evolved_rename_expr ON evolved_rename ((lower(label)))")
    connection.query("INSERT INTO evolved_rename VALUES (1, 'Kept')")
    statement = b"before_column_rename"
    assert cycle(connection, [parse("SELECT label FROM evolved_rename", (), name=statement)],
                 with_message=True) == []
    connection.query("BEGIN")
    connection.query("ALTER TABLE evolved_rename RENAME COLUMN label TO provisional")
    assert connection.query("SELECT provisional FROM evolved_rename") == [["Kept"]]
    connection.query("ROLLBACK")
    assert connection.query("SELECT label FROM evolved_rename") == [["Kept"]]
    connection.query("ALTER TABLE evolved_rename RENAME COLUMN label TO display")
    errors = cycle(connection, [bind((), statement=statement), frame(b"E", bytes(5))])
    assert errors == [(b"E", "42703")], errors
    try:
        connection.query("UPDATE evolved_rename SET display = 'forbidden'")
    except WireError as error:
        assert error.sqlstate == "23514", error
    else:
        raise AssertionError("renaming the column disabled its CHECK constraint")

    connection.query("CREATE TABLE evolved_required (id integer PRIMARY KEY, value integer)")
    connection.query("INSERT INTO evolved_required VALUES (1, NULL)")
    try:
        connection.query("ALTER TABLE evolved_required ALTER COLUMN value SET NOT NULL")
    except WireError as error:
        assert error.sqlstate == "23502", error
    else:
        raise AssertionError("SET NOT NULL accepted a stored NULL")
    connection.query("UPDATE evolved_required SET value=7")
    connection.query("ALTER TABLE evolved_required ALTER COLUMN value SET NOT NULL")
    try:
        connection.query("INSERT INTO evolved_required VALUES (2, NULL)")
    except WireError as error:
        assert error.sqlstate == "23502", error
    else:
        raise AssertionError("validated NOT NULL accepted a new NULL")
    connection.query("BEGIN")
    connection.query("ALTER TABLE evolved_required ALTER COLUMN value DROP NOT NULL")
    connection.query("INSERT INTO evolved_required VALUES (2, NULL)")
    connection.query("COMMIT")
    connection.query("CREATE TABLE evolved_batch (id integer PRIMARY KEY, v integer)")
    connection.query("INSERT INTO evolved_batch VALUES (1, NULL)")
    try:
        connection.query(
            "ALTER TABLE evolved_batch ADD COLUMN tentative integer, ALTER COLUMN v SET NOT NULL"
        )
    except WireError as error:
        assert error.sqlstate == "23502", error
    else:
        raise AssertionError("multi-action validation accepted a stored NULL")
    assert connection.query("SELECT * FROM evolved_batch") == [["1", None]]
    connection.query(
        "ALTER TABLE evolved_batch ALTER COLUMN flag SET DEFAULT true, "
        "ALTER COLUMN flag SET NOT NULL, ADD COLUMN flag boolean DEFAULT false"
    )
    connection.query("INSERT INTO evolved_batch (id) VALUES (2)")
    verify_restored_column_addition(connection)


def verify_restored_column_addition(connection):
    assert connection.query("SELECT * FROM evolved_heap ORDER BY id") == [
        ["1", None, None],
        ["3", "f", "committed"],
    ]
    assert connection.query("SELECT * FROM evolved_defaults ORDER BY id") == [
        ["1", "f"],
        ["2", None],
        ["3", "t"],
        ["4", None],
    ]
    assert connection.query("SELECT display FROM evolved_rename WHERE lower(display)='kept'") == [["Kept"]]
    assert connection.query("SELECT * FROM evolved_required ORDER BY id") == [["1", "7"], ["2", None]]
    assert connection.query("SELECT flag FROM evolved_batch ORDER BY id") == [["f"], ["t"]]
    assert connection.query(
        "SELECT a.attnum, a.atthasmissing, a.attisdropped "
        "FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid=c.oid "
        "WHERE c.relname='evolved_defaults' AND a.attname='flag'"
    ) == [["2", "t", "f"]]


def verify_column_drop(connection):
    connection.query(
        "CREATE TABLE evolved_drop (id integer PRIMARY KEY, gone text CHECK (gone <> ''), tail integer)"
    )
    connection.query("CREATE INDEX evolved_drop_expression ON evolved_drop ((length(gone)))")
    connection.query("ALTER TABLE evolved_drop ALTER COLUMN gone SET STORAGE EXTERNAL")
    connection.query("INSERT INTO evolved_drop VALUES (1, '" + ("abcdefgh" * 8000) + "', 7)")
    connection.query("CREATE PUBLICATION evolved_drop_pub FOR TABLE evolved_drop WHERE (gone <> '')")
    try:
        connection.query("ALTER TABLE evolved_drop DROP COLUMN gone RESTRICT")
    except WireError as error:
        assert error.sqlstate == "2BP01", error
    else:
        raise AssertionError("DROP COLUMN ignored the publication dependency")
    connection.query("BEGIN")
    connection.query("ALTER TABLE evolved_drop DROP COLUMN gone CASCADE")
    assert connection.query("SELECT relid FROM pg_get_publication_tables('evolved_drop_pub')") == []
    assert connection.query("SELECT * FROM evolved_drop") == [["1", "7"]]
    connection.query("ROLLBACK")
    assert connection.query("SELECT length(gone), tail FROM evolved_drop") == [["64000", "7"]]
    try:
        connection.query("ALTER TABLE evolved_drop DROP COLUMN gone RESTRICT")
    except WireError as error:
        assert error.sqlstate == "2BP01", error
    else:
        raise AssertionError("rollback lost the publication dependency")
    connection.query("ALTER TABLE evolved_drop DROP COLUMN gone CASCADE")
    assert connection.query("SELECT relid FROM pg_get_publication_tables('evolved_drop_pub')") == []
    assert connection.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relname='evolved_drop_expression'"
    ) == []
    connection.query("UPDATE evolved_drop SET tail=8 WHERE id=1")
    connection.query("INSERT INTO evolved_drop VALUES (2, 9)")
    connection.query("VACUUM FULL evolved_drop")
    connection.query("ALTER TABLE evolved_drop ADD COLUMN gone integer DEFAULT 42")
    verify_restored_column_drop(connection)


def verify_restored_column_drop(connection):
    assert connection.query("SELECT relid FROM pg_get_publication_tables('evolved_drop_pub')") == []
    assert connection.query("SELECT * FROM evolved_drop ORDER BY id") == [
        ["1", "8", "42"],
        ["2", "9", "42"],
    ]
    assert connection.query(
        "SELECT a.attnum, a.attisdropped FROM pg_catalog.pg_attribute a "
        "JOIN pg_catalog.pg_class c ON a.attrelid=c.oid "
        "WHERE c.relname='evolved_drop' AND a.attnum > 0 ORDER BY a.attnum"
    ) == [["1", "f"], ["2", "t"], ["3", "f"], ["4", "f"]]


def verify_column_type_rewrite(connection):
    connection.query("CREATE TABLE evolved_rewrite (id integer PRIMARY KEY, code text, payload text)")
    connection.query("ALTER TABLE evolved_rewrite ALTER COLUMN payload SET STORAGE EXTERNAL")
    connection.query("CREATE INDEX evolved_rewrite_code ON evolved_rewrite(code)")
    connection.query(
        "INSERT INTO evolved_rewrite VALUES (1,'42','" + "abcdefgh" * 8000 + "'),(2,'bad','ok')"
    )
    try:
        connection.query("ALTER TABLE evolved_rewrite ALTER COLUMN code TYPE integer USING code::integer")
    except WireError as error:
        assert error.sqlstate == "22P02", error
    else:
        raise AssertionError("ALTER TYPE accepted an invalid integer")
    assert connection.query("SELECT code FROM evolved_rewrite ORDER BY id") == [["42"], ["bad"]]
    connection.query("UPDATE evolved_rewrite SET code='43' WHERE id=2")
    try:
        connection.query("ALTER TABLE evolved_rewrite ALTER COLUMN id TYPE bigint USING 1")
    except WireError as error:
        assert error.sqlstate == "23505", error
    else:
        raise AssertionError("ALTER TYPE published duplicate primary keys")
    assert connection.query("SELECT id FROM evolved_rewrite ORDER BY id") == [["1"], ["2"]]
    connection.query("BEGIN")
    connection.query("ALTER TABLE evolved_rewrite ALTER COLUMN id SET DATA TYPE bigint")
    assert connection.query("SELECT id, code FROM evolved_rewrite WHERE id=2") == [["2", "43"]]
    connection.query("ROLLBACK")
    connection.query("ALTER TABLE evolved_rewrite ALTER COLUMN code TYPE integer USING code::integer")
    connection.query("ALTER TABLE evolved_rewrite ALTER COLUMN id TYPE bigint")
    verify_restored_column_type_rewrite(connection)


def verify_restored_column_type_rewrite(connection):
    assert connection.query(
        "SELECT id, length(payload) FROM evolved_rewrite WHERE code=42"
    ) == [["1", "64000"]]
    assert connection.query(
        "SELECT a.attname, a.atttypid FROM pg_catalog.pg_attribute a "
        "JOIN pg_catalog.pg_class c ON a.attrelid=c.oid "
        "WHERE c.relname='evolved_rewrite' AND a.attnum > 0 ORDER BY a.attnum"
    ) == [["id", "20"], ["code", "23"], ["payload", "25"]]


def verify_column_type_rewrite_invalid_index(connection):
    connection.query("CREATE TABLE invalid_index_rewrite (id integer, code integer)")
    connection.query("INSERT INTO invalid_index_rewrite VALUES (1, 7), (2, 7)")
    try:
        connection.query("CREATE UNIQUE INDEX CONCURRENTLY invalid_rewrite_ix ON invalid_index_rewrite(code)")
    except WireError as error:
        assert error.sqlstate == "23505", error
    else:
        raise AssertionError("concurrent unique index accepted duplicate input")
    index_state = (
        "SELECT i.indisvalid, i.indisready FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
        "WHERE c.relname='invalid_rewrite_ix'"
    )
    before = connection.query(index_state)
    assert before == [["f", "f"]]
    connection.query("ALTER TABLE invalid_index_rewrite ALTER COLUMN id TYPE bigint")
    assert connection.query(index_state) == before


def verify_column_type_rewrite_failure(server, connection, fault, sqlstate):
    table = f"{fault}_rewrite"
    connection.query(f"CREATE TABLE {table} (id integer PRIMARY KEY)")
    connection.query(f"INSERT INTO {table} VALUES (1), (2)")
    column_type = (
        "SELECT a.atttypid FROM pg_catalog.pg_attribute a "
        "JOIN pg_catalog.pg_class c ON a.attrelid=c.oid "
        f"WHERE c.relname='{table}' AND a.attnum=1"
    )
    alter = f"ALTER TABLE {table} ALTER COLUMN id TYPE bigint"
    cancel = """(prog1 (apply insert args)
                   (pg::cancel-backend pg::*backend*))"""
    exhaust_scratch = """(let ((pg::*wal-dirty-resident-limit-pages*
                                  (if wrote-row 0 pg::*wal-dirty-resident-limit-pages*)))
                            (prog1 (apply insert args)
                              (setf wrote-row t)))"""
    action = cancel if fault == "cancel" else exhaust_scratch
    server.control(f"""
        (defparameter pg::*test-original-heapam-insert* (fdefinition 'pg::heapam-insert))
        (defparameter pg::*test-original-scratch* (fdefinition 'pg::wal-dirty-ensure-spool))
        (when (string= "{fault}" "scratch")
          (setf (fdefinition 'pg::wal-dirty-ensure-spool)
                (lambda (store)
                  (declare (ignore store))
                  ;; The isolated fault performs a real write that returns ENOSPC.
                  (with-open-file (sink #p"/dev/full" :direction :output
                                        :if-exists :overwrite
                                        :element-type '(unsigned-byte 8))
                    (write-byte 0 sink)
                    (finish-output sink)))))
        (setf (fdefinition 'pg::heapam-insert)
              (let ((insert pg::*test-original-heapam-insert*)
                    (wrote-row nil))
                (declare (ignorable wrote-row))
                (lambda (&rest args)
                  (if (pg::current-relation-replacement (first args) :main)
                      {action}
                      (apply insert args)))))
    """)
    try:
        try:
            connection.query(alter)
        except WireError as error:
            assert error.sqlstate == sqlstate, error
            if fault == "scratch":
                assert "No space left on device" in str(error), error
        else:
            raise AssertionError(f"ALTER TYPE ignored the {fault} fault")
    finally:
        server.control("""
            (setf (fdefinition 'pg::heapam-insert) pg::*test-original-heapam-insert*)
            (setf (fdefinition 'pg::wal-dirty-ensure-spool) pg::*test-original-scratch*)
            (makunbound 'pg::*test-original-heapam-insert*)
            (makunbound 'pg::*test-original-scratch*)
        """)
    assert connection.query(column_type) == [["23"]]
    assert connection.query(f"SELECT id FROM {table} ORDER BY id") == [["1"], ["2"]]
    connection.query(alter)
    assert connection.query(column_type) == [["20"]]
    assert connection.query(f"SELECT id FROM {table} WHERE id=2") == [["2"]]


def verify_column_validation_waits(server):
    with contextlib.closing(Connection(server.port)) as writer, contextlib.closing(
        Connection(server.port)
    ) as validator:
        writer.query("CREATE TABLE validate_evolution (value integer)")
        writer.query("BEGIN")
        writer.query("INSERT INTO validate_evolution VALUES (NULL)")
        alter = "ALTER TABLE validate_evolution ALTER COLUMN value SET NOT NULL"
        with ThreadPoolExecutor(max_workers=1) as worker:
            result = worker.submit(validator.query, alter)
            waiting = False
            try:
                deadline = time.monotonic() + 5
                while not result.done() and time.monotonic() < deadline:
                    observed = server.control(
                        '(format t "~&COLUMN-VALIDATION-WAIT=~a~%" '
                        '(or (sb-thread:with-mutex (pg::*schema-lock-mutex*) '
                        '(plusp pg::*schema-write-lock-waiters*)) '
                        '(sb-thread:with-mutex (pg::*relation-lock-mutex*) '
                        '(plusp (hash-table-count pg::*relation-wait-dependencies*)))))'
                    )
                    if "COLUMN-VALIDATION-WAIT=T" in observed:
                        waiting = True
                        break
            finally:
                writer.query("COMMIT")
            assert waiting, "SET NOT NULL did not wait for the earlier writer"
            try:
                result.result(timeout=10)
            except WireError as error:
                assert error.sqlstate == "23502", error
            else:
                raise AssertionError("validation missed the NULL committed by the earlier writer")

        writer.query("UPDATE validate_evolution SET value=7")
        writer.query("SET statement_timeout = '200ms'")
        validator.query("BEGIN")
        validator.query(alter)
        try:
            writer.query("INSERT INTO validate_evolution VALUES (NULL)")
        except WireError as error:
            assert error.sqlstate == "57014", error
        else:
            raise AssertionError("a writer crossed the validation gate before commit")
        finally:
            validator.query("COMMIT")
        writer.query("RESET statement_timeout")
        try:
            writer.query("INSERT INTO validate_evolution VALUES (NULL)")
        except WireError as error:
            assert error.sqlstate == "23502", error
        else:
            raise AssertionError("the committed descriptor accepted a NULL")


def verify_logical_column_type_rewrite():
    source, target = OwnedLisp(), OwnedLisp()
    try:
        source.start()
        target.start()
        with contextlib.closing(Connection(source.port)) as writer, contextlib.closing(
            Connection(target.port)
        ) as reader:
            writer.query("CREATE TABLE logical_retype (discarded text, id integer PRIMARY KEY, code text)")
            writer.query("ALTER TABLE logical_retype ALTER COLUMN discarded SET STORAGE EXTERNAL")
            writer.query("INSERT INTO logical_retype VALUES ('discarded',1,'42')")
            writer.query("ALTER TABLE logical_retype DROP COLUMN discarded")
            writer.query("CREATE PUBLICATION logical_retype_pub FOR TABLE logical_retype")
            assert writer.query(
                "SELECT attrs::text FROM pg_get_publication_tables('logical_retype_pub')"
            ) == [["2 3"]]
            subscriber = Path(__file__).resolve().parents[1] / "pg-lisp-subscribe"
            with (target.root / "subscriber.log").open("w") as log, subprocess.Popen(
                [str(subscriber), "--slot", "logical_retype_slot", "--table", "logical_retype",
                 "--source-port", str(source.port), "--target-port", str(target.port)],
                stdout=log, stderr=subprocess.STDOUT,
            ) as worker:
                def await_rows(connection, sql, expected):
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline and worker.poll() is None:
                        try:
                            if connection.query(sql) == expected:
                                return
                        except WireError as error:
                            assert error.sqlstate == "42P01", error
                        time.sleep(0.05)
                    raise AssertionError((expected, (target.root / "subscriber.log").read_text()))

                rows = "SELECT id, code FROM logical_retype ORDER BY id"
                try:
                    await_rows(reader, rows, [["1", "42"]])
                    await_rows(writer,
                               "SELECT plugin, confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots "
                               "WHERE slot_name='logical_retype_slot'",
                               [["pg_lisp_transactional", "t"]])
                    try:
                        writer.query(
                            "ALTER TABLE logical_retype ALTER COLUMN id TYPE bigint USING txid_current()"
                        )
                    except WireError as error:
                        assert error.sqlstate == "42P17" and "IMMUTABLE" in str(error), error
                    else:
                        raise AssertionError("ALTER TYPE accepted a transaction-dependent expression")
                    writer.query("SET datestyle = 'ISO, DMY'")
                    try:
                        writer.query(
                            "ALTER TABLE logical_retype ALTER COLUMN id TYPE bigint USING "
                            "CASE WHEN '01/02/2000'::date = '2000-02-01'::date THEN 1 ELSE 2 END"
                        )
                    except WireError as error:
                        assert error.sqlstate == "0A000" and "temporal literals" in str(error), error
                    else:
                        raise AssertionError("ALTER TYPE accepted a session-dependent literal")
                    finally:
                        writer.query("RESET datestyle")
                    writer.query("BEGIN")
                    writer.query("INSERT INTO logical_retype VALUES (2,'43')")
                    writer.query(
                        "ALTER TABLE logical_retype ALTER COLUMN code TYPE integer USING code::integer"
                    )
                    writer.query("INSERT INTO logical_retype VALUES (3,44)")
                    writer.query("UPDATE logical_retype SET code=45 WHERE id=3")
                    writer.query("DELETE FROM logical_retype WHERE id=1")
                    writer.query("COMMIT")
                    await_rows(reader, rows, [["2", "43"], ["3", "45"]])
                    assert reader.query(
                        "SELECT a.atttypid FROM pg_catalog.pg_attribute a "
                        "JOIN pg_catalog.pg_class c ON a.attrelid=c.oid "
                        "WHERE c.relname='logical_retype' AND a.attname='code'"
                    ) == [["23"]]
                finally:
                    worker.terminate()
                    try:
                        worker.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        worker.kill()
                        worker.wait()
    finally:
        target.stop()
        source.stop()



def main():
    server = OwnedLisp()
    try:
        server.start()
        verify_create(server)
        verify_drop(server)
        verify_indexes(server)
        verify_constraint_indexes(server)
        with contextlib.closing(Connection(server.port)) as connection:
            verify_column_addition(connection)
            verify_column_drop(connection)
            verify_column_type_rewrite(connection)
            verify_column_type_rewrite_invalid_index(connection)
            verify_column_type_rewrite_failure(server, connection, "cancel", "57014")
            verify_column_type_rewrite_failure(server, connection, "scratch", "58030")
            verify_implicit_catalog_block(connection)
            verify_expression_index(connection)
            verify_expression_index_plan(connection)
            verify_partial_range_index(connection)
            verify_expression_index_rebind(server, connection)
        verify_column_validation_waits(server)
        verify_new_logical_interest(server)
        verify_first_slot_activation_during_ddl(server)
        verify_net_zero_ddl_slot_admission(server)
        verify_apply_requires_schema_contract(server)
        verify_commit_dependency_races(server)
        verify_legacy_slot_noops(server)
        verify_ordered_catalog_capture(server)
        transactional_birth = verify_transactional_slot_handshake(server)
        server.stop(immediate=True)
        server.start()
        verify_catalog_capture_records(server)
        with contextlib.closing(Connection(server.port)) as connection:
            assert connection.query(
                "SELECT plugin,confirmed_flush_lsn FROM pg_replication_slots "
                "WHERE slot_name='transactional_slot'"
            ) == [["pg_lisp_transactional", transactional_birth]]
            connection.query("SELECT pg_drop_replication_slot('catalog_capture')")
            verify_committed(connection)
            verify_committed_index(connection)
            verify_committed_constraints(connection)
            verify_restored_column_addition(connection)
            verify_restored_column_drop(connection)
            verify_restored_column_type_rewrite(connection)
            verify_restored_expression_index(connection)
            assert connection.query("SELECT id FROM implicit_heap") == [["9"]]
            absent(connection, "implicit_abort")
        verify_transactional_slot_resume(server, transactional_birth)
        verify_crash_boundary(server, "after-page-images", "uncommitted_heap", False)
        verify_crash_boundary(
            server, "after-catalog-transaction-wal-flush", "durable_heap", True
        )
        verify_expression_index_crash_boundaries(server)
        verify_logical_column_type_rewrite()
    finally:
        server.stop()
        shutil.rmtree(server.root)


if __name__ == "__main__":
    main()
