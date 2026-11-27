"""Verify transactional catalog visibility and recovery over the SQL wire protocol."""

import contextlib
import runpy
import shutil
import struct
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

def main():
    server = OwnedLisp()
    try:
        server.start()
        verify_create(server)
        verify_drop(server)
        verify_indexes(server)
        verify_constraint_indexes(server)
        with contextlib.closing(Connection(server.port)) as connection:
            verify_implicit_catalog_block(connection)
            verify_expression_index(connection)
            verify_expression_index_plan(connection)
            verify_partial_range_index(connection)
            verify_expression_index_rebind(server, connection)
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
            verify_restored_expression_index(connection)
            assert connection.query("SELECT id FROM implicit_heap") == [["9"]]
            absent(connection, "implicit_abort")
        verify_transactional_slot_resume(server, transactional_birth)
        verify_crash_boundary(server, "after-page-images", "uncommitted_heap", False)
        verify_crash_boundary(
            server, "after-catalog-transaction-wal-flush", "durable_heap", True
        )
        verify_expression_index_crash_boundaries(server)
    finally:
        server.stop()
        shutil.rmtree(server.root)


if __name__ == "__main__":
    main()
