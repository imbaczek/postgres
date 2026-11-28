"""Verify transactional catalog visibility and recovery over the SQL wire protocol."""

import contextlib
from concurrent.futures import ThreadPoolExecutor
import json
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


def verify_view_transaction_visibility(port):
    with contextlib.closing(Connection(port)) as writer, contextlib.closing(
        Connection(port)
    ) as reader:
        writer.query("CREATE TABLE view_tx_base (seed int)")
        writer.query("INSERT INTO view_tx_base VALUES (1), (2), (3)")
        expected = writer.query("SELECT seed + 1 FROM view_tx_base WHERE seed > 1 ORDER BY 1")
        catalog = (
            "SELECT relname FROM pg_class WHERE relname IN ('view_tx','view_tx_child') "
            "ORDER BY relname"
        )
        present = [["view_tx"], ["view_tx_child"]]
        for outcome in ("ROLLBACK", "COMMIT"):
            writer.query("BEGIN")
            writer.query("CREATE VIEW view_tx (id) AS SELECT seed FROM view_tx_base WHERE seed > 1")
            writer.query("CREATE VIEW view_tx_child (id) AS SELECT id + 1 FROM view_tx")
            assert writer.query("SELECT id FROM view_tx_child ORDER BY id") == expected
            assert reader.query(catalog) == []
            absent(reader, "view_tx_child")
            writer.query(outcome)
        assert reader.query(catalog) == present
        assert reader.query("SELECT id FROM view_tx_child ORDER BY id") == expected

        for outcome in ("ROLLBACK", "COMMIT"):
            writer.query("BEGIN")
            writer.query("DROP VIEW view_tx CASCADE")
            assert writer.query(catalog) == []
            assert reader.query(catalog) == present
            writer.query(outcome)
            if outcome == "ROLLBACK":
                assert reader.query("SELECT id FROM view_tx_child ORDER BY id") == expected
        assert reader.query(catalog) == []
        absent(reader, "view_tx_child")
        absent(reader, "view_tx")


def verify_view_read_only_and_acyclic(port):
    with contextlib.closing(Connection(port)) as reader:
        reader.query("CREATE TABLE view_guard_base (id int)")
        reader.query("INSERT INTO view_guard_base VALUES (7)")
        reader.query("CREATE VIEW view_guard AS SELECT id FROM view_guard_base")
        reader.query("CREATE VIEW view_guard_child AS SELECT id FROM view_guard")
        for sql, sqlstate in (
            ("CREATE OR REPLACE VIEW view_guard AS SELECT id FROM view_guard_child", "42P17"),
            ("INSERT INTO view_guard VALUES (8)", "42809"),
            ("UPDATE view_guard SET id = 8", "42809"),
            ("DELETE FROM view_guard", "42809"),
            ("TRUNCATE view_guard", "42809"),
        ):
            try:
                reader.query(sql)
            except WireError as error:
                assert error.sqlstate == sqlstate, error
            else:
                raise AssertionError(f"unexpectedly accepted: {sql}")
        assert reader.query("SELECT id FROM view_guard_child") == [["7"]]
        assert reader.query("SELECT id FROM view_guard_base") == [["7"]]


def verify_view_definition_context(port, *, create=False):
    with contextlib.closing(Connection(port)) as reader:
        if create:
            reader.query("SET TIME ZONE 'UTC'")
            reader.query(
                "CREATE VIEW view_typed_temporal_array AS SELECT "
                "'[0:1]={\"2000-01-01 00:00:00\",NULL}'::timestamptz[] AS instants"
            )
            reader.query(
                "CREATE VIEW view_definition_context AS "
                "SELECT * FROM (SELECT '2000-01-01 00:00:00'::timestamptz AS instant) AS logged"
            )
            reader.query("CREATE TABLE view_temporal_source (instant timestamptz)")
            reader.query("INSERT INTO view_temporal_source VALUES ('2000-01-01 00:00:00+00')")
            reader.query(
                "CREATE VIEW view_implicit_context AS SELECT instant FROM view_temporal_source "
                "WHERE instant = '2000-01-01 00:00:00'"
            )
            reader.query(
                "CREATE VIEW view_array_context AS SELECT instant FROM view_temporal_source "
                "WHERE instant = ANY ('{2000-01-01 00:00:00}')"
            )
            reader.query(
                "CREATE VIEW view_union_context AS SELECT '2000-01-01 00:00:00' AS instant "
                "UNION ALL SELECT '2000-01-02 00:00:00+00'::timestamptz"
            )
            reader.query("SET search_path = pg_catalog")
            reader.query("DROP VIEW IF EXISTS view_definition_context")
        reader.query("SET TIME ZONE 'America/New_York'")
        array_test = (
            "SELECT instants[0] = '2000-01-01 00:00:00+00'::timestamptz, "
            "instants[1] IS NULL FROM "
        )
        assert reader.query(array_test + "public.view_typed_temporal_array") == [["t", "t"]]
        if create:
            reader.query("SET search_path = public")
            definition = reader.query(
                "SELECT pg_get_viewdef(oid) FROM pg_catalog.pg_class "
                "WHERE relname = 'view_typed_temporal_array'"
            )[0][0]
            reader.query("CREATE VIEW view_temporal_array_copy AS " + definition)
            assert reader.query(array_test + "view_temporal_array_copy") == [["t", "t"]]
        assert reader.query(
            "SELECT instant = '2000-01-01 00:00:00+00'::timestamptz "
            "FROM public.view_definition_context"
        ) == [["t"]]
        assert reader.query("SELECT count(*) FROM public.view_implicit_context") == [["1"]]
        assert reader.query("SELECT count(*) FROM public.view_array_context") == [["1"]]
        assert reader.query(
            "SELECT count(*) FROM public.view_union_context "
            "WHERE instant = '2000-01-01 00:00:00+00'::timestamptz"
        ) == [["1"]]
        statement = b"view_context_parameter"
        assert cycle(reader, [
            parse("SELECT count(*) FROM public.view_implicit_context WHERE instant = $1",
                  (1184,), name=statement),
        ], with_message=True) == []
        for instant, expected in ((b"2000-01-01 00:00:00+00", 1), (None, 0)):
            messages = cycle(reader, [bind((instant,), statement=statement), frame(b"E", bytes(5))])
            assert all(kind != b"E" for kind, _ in messages), messages
            assert [int(body[6:]) for kind, body in messages if kind == b"D"] == [expected]


def verify_whole_row_view_definition(port, *, create=False, duplicate_roundtrip=True):
    with contextlib.closing(Connection(port)) as reader:
        if create:
            reader.query("CREATE TABLE view_row_source (id int, payload int, expendable int)")
            reader.query("INSERT INTO view_row_source VALUES (1, 2, 3)")
            reader.query(
                "CREATE VIEW view_whole_row AS SELECT id, row_to_json(view_row_source) AS doc "
                "FROM view_row_source"
            )
            reader.query(
                "CREATE VIEW view_derived_row AS SELECT row_to_json(d) AS doc "
                "FROM (SELECT id, payload FROM view_row_source) AS d"
            )
            reader.query("CREATE VIEW view_record_target AS SELECT view_row_source AS rec FROM view_row_source")
            reader.query(
                "CREATE VIEW view_duplicate_row AS SELECT a.n, d.tag, row_to_json(d) AS doc "
                "FROM (SELECT 1 AS n UNION ALL SELECT 2) AS a "
                "LEFT JOIN (SELECT id AS key, id + 10 AS key, payload AS tag "
                "FROM view_row_source UNION ALL SELECT NULL::int, NULL::int, NULL::int) "
                "AS d ON a.n = 1"
            )
            reader.query(
                "CREATE VIEW view_selected_row_field AS "
                "SELECT (view_row_source).expendable AS field, "
                "(SELECT (view_row_source).expendable) AS outer_field FROM view_row_source"
            )
            reader.query("ALTER TABLE view_row_source RENAME COLUMN id TO renamed")
            reader.query("ALTER TABLE view_row_source ADD COLUMN r0_1 int DEFAULT 5")
            reader.query("ALTER TABLE view_row_source RENAME COLUMN expendable TO unwanted")
            assert reader.query("SELECT field, outer_field FROM view_selected_row_field") == [["3", "3"]]
            try:
                reader.query("ALTER TABLE view_row_source DROP COLUMN unwanted")
            except WireError as error:
                assert error.sqlstate == "2BP01", error
            else:
                raise AssertionError("dropped a field explicitly selected from a whole row")
            reader.query("DROP VIEW view_selected_row_field")
            reader.query("ALTER TABLE view_row_source DROP COLUMN unwanted")
        for name, expected in (
            ("view_whole_row", {"renamed": 1, "payload": 2, "r0_1": 5}),
            ("view_derived_row", {"id": 1, "payload": 2}),
        ):
            assert json.loads(reader.query(f"SELECT doc FROM {name}")[0][0]) == expected
            definition = reader.query(
                f"SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = '{name}'"
            )[0][0]
            reader.query(f"CREATE VIEW view_row_copy AS {definition}")
            assert reader.query(f"SELECT * FROM {name}") == reader.query("SELECT * FROM view_row_copy")
            reader.query("DROP VIEW view_row_copy")
        for name, suffix in (("view_record_target", ""), ("view_duplicate_row", " ORDER BY n, tag NULLS LAST")):
            original = reader.query(f"SELECT * FROM {name}" + suffix)
            if name == "view_duplicate_row":
                assert original == [
                    ["1", "2", '{"key":1,"key":11,"tag":2}'],
                    ["1", None, '{"key":null,"key":null,"tag":null}'],
                    ["2", None, None],
                ]
                if not duplicate_roundtrip:
                    # C's deparser disambiguates the field aliases on recreation.
                    continue
            definition = reader.query(
                f"SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = '{name}'"
            )[0][0]
            reader.query(f"CREATE VIEW view_row_copy AS {definition}")
            assert reader.query("SELECT * FROM view_row_copy" + suffix) == original
            if create and name == "view_duplicate_row":
                reader.query("ALTER TABLE view_row_source RENAME COLUMN renamed TO renamed_again")
                emitted = reader.query(
                    "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'view_row_copy'"
                )[0][0]
                reader.query("CREATE VIEW view_row_copy_again AS " + emitted)
                assert reader.query("SELECT * FROM view_row_copy_again" + suffix) == original
                reader.query("DROP VIEW view_row_copy_again")
                reader.query("ALTER TABLE view_row_source RENAME COLUMN renamed_again TO renamed")
            reader.query("DROP VIEW view_row_copy")


def verify_view_sequence_dependencies(port, *, create=False, race=False):
    with contextlib.closing(Connection(port)) as reader:
        if create:
            reader.query("CREATE SEQUENCE view_sequence_source")
            reader.query("CREATE VIEW view_sequence AS SELECT nextval('view_sequence_source') AS number")
            assert reader.query("SELECT number FROM view_sequence") == [["1"]]
            reader.query("CREATE VIEW view_currval AS SELECT currval('view_sequence_source') AS number")
            assert reader.query("SELECT number FROM view_currval") == [["1"]]
            reader.query("CREATE VIEW view_setval AS SELECT setval('view_sequence_source', 5) AS number")
            assert reader.query("SELECT number FROM view_setval") == [["5"]]
            reader.query("CREATE SEQUENCE view_sequence_late")
            reader.query(
                "CREATE VIEW view_late_sequence AS "
                "SELECT nextval('view_sequence_late'::text) AS number"
            )
            reader.query("DROP SEQUENCE view_sequence_late")
            reader.query("CREATE SEQUENCE view_sequence_late START WITH 40")
            assert reader.query("SELECT number FROM view_late_sequence") == [["40"]]
        try:
            reader.query("DROP SEQUENCE view_sequence_source")
        except WireError as error:
            assert error.sqlstate == "2BP01", error
        else:
            raise AssertionError("dropped a sequence bound by a view")
        definition = reader.query(
            "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'view_sequence'"
        )[0][0]
        reader.query(f"CREATE VIEW view_sequence_copy AS {definition}")
        reader.query("DROP VIEW view_sequence")
        if create:
            reader.query("DROP VIEW view_currval, view_setval")
        try:
            reader.query("DROP SEQUENCE view_sequence_source")
        except WireError as error:
            assert error.sqlstate == "2BP01", error
        else:
            raise AssertionError("the recreated view lost its sequence dependency")
        reader.query(f"CREATE VIEW view_sequence AS {definition}")
        reader.query("DROP VIEW view_sequence_copy")
        if race:
            reader.query("CREATE SEQUENCE view_sequence_race")
            reader.query("BEGIN")
            reader.query("CREATE VIEW view_racing_sequence AS SELECT nextval('view_sequence_race')")
            with contextlib.closing(Connection(port)) as concurrent:
                concurrent.query("DROP SEQUENCE view_sequence_race")
                concurrent.query("CREATE SEQUENCE view_sequence_race")
            try:
                reader.query("COMMIT")
            except WireError as error:
                assert error.sqlstate == "40001", error
            else:
                raise AssertionError("view commit accepted a replaced sequence")


def verify_view_nonsequence_dependencies(port, *, create=False, race=False):
    with contextlib.closing(Connection(port)) as reader:
        if create:
            reader.query("CREATE TABLE view_sequence_table (id int)")
            reader.query("CREATE TABLE view_index_owner (id int)")
            reader.query("CREATE INDEX view_sequence_index ON view_index_owner (id)")
            reader.query("CREATE VIEW view_table_regclass AS SELECT nextval('view_sequence_table') AS n")
            reader.query("CREATE VIEW view_index_regclass AS SELECT nextval('view_sequence_index') AS n")
            reader.query("CREATE INDEX view_sequence_drop_ix ON view_index_owner (id)")
            reader.query("CREATE VIEW view_index_direct_drop AS SELECT nextval('view_sequence_drop_ix') AS n")
            reader.query("DROP INDEX view_sequence_drop_ix CASCADE")
            absent(reader, "view_index_direct_drop")
            reader.query("CREATE TABLE view_constraint_owner (id int CONSTRAINT view_backing UNIQUE)")
            reader.query("CREATE VIEW view_constraint_regclass AS SELECT nextval('view_backing') AS n")
            reader.query("CREATE INDEX view_sequence_rename_ix ON view_index_owner (id)")
            reader.query("CREATE VIEW view_index_rename_ref AS SELECT nextval('view_sequence_rename_ix') AS n")
            reader.query("ALTER INDEX view_sequence_rename_ix RENAME TO view_sequence_renamed_ix")
            reader.query("CREATE SEQUENCE view_sequence_rename_ix START WITH 9")
            definition = reader.query(
                "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'view_index_rename_ref'"
            )[0][0]
            reader.query("CREATE VIEW view_index_renamed_copy AS " + definition)
            reader.query("DROP VIEW view_index_rename_ref")
            assert reader.query("SELECT nextval('view_sequence_rename_ix')") == [["9"]]
            reader.query("CREATE SEQUENCE view_numeric_oid_source")
            oid = reader.query("SELECT oid FROM pg_class WHERE relname = 'view_numeric_oid_source'")[0][0]
            reader.query(f"CREATE VIEW view_numeric_oid AS SELECT nextval('{oid}') AS n")
            assert reader.query("SELECT n FROM view_numeric_oid") == [["1"]]
            system_oid = reader.query("SELECT oid FROM pg_class WHERE relname = 'pg_class'")[0][0]
            reader.query(f"CREATE VIEW view_system_regclass AS SELECT nextval('{system_oid}') AS n")
            definition = reader.query(
                "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'view_system_regclass'"
            )[0][0]
            reader.query("CREATE VIEW view_system_regclass_copy AS " + definition)
            reader.query('CREATE SEQUENCE "123"')
            reader.query("CREATE VIEW view_numeric_oid_missing AS SELECT nextval('123') AS n")
            reader.query('DROP SEQUENCE "123"')
        for statement, state in (
            ("SELECT n FROM view_table_regclass", "42809"),
            ("SELECT n FROM view_index_regclass", "42809"),
            ("DROP TABLE view_sequence_table", "2BP01"),
            ("DROP INDEX view_sequence_index", "2BP01"),
            ("DROP INDEX CONCURRENTLY view_sequence_index", "2BP01"),
            ("DROP TABLE view_index_owner", "2BP01"),
            ("ALTER TABLE view_constraint_owner DROP CONSTRAINT view_backing", "2BP01"),
            ("SELECT n FROM view_index_renamed_copy", "42809"),
            ("DROP INDEX view_sequence_renamed_ix", "2BP01"),
            ("DROP SEQUENCE view_numeric_oid_source", "2BP01"),
            ("SELECT n FROM view_numeric_oid_missing", "XX000"),
            ("SELECT n FROM view_system_regclass_copy", "42809"),
        ):
            try:
                reader.query(statement)
            except WireError as error:
                assert error.sqlstate == state, error
            else:
                raise AssertionError(f"unexpectedly accepted: {statement}")
        if not create:
            reader.query("DROP TABLE view_sequence_table, view_index_owner CASCADE")
            absent(reader, "view_table_regclass")
            absent(reader, "view_index_regclass")
        if race:
            reader.query("CREATE TABLE view_index_race_table (id int)")
            reader.query("CREATE INDEX view_index_race ON view_index_race_table (id)")
            reader.query("BEGIN")
            reader.query("DROP INDEX view_index_race")
            with contextlib.closing(Connection(port)) as other:
                other.query("CREATE VIEW view_index_race_ref AS SELECT nextval('view_index_race') AS n")
            try:
                reader.query("COMMIT")
            except WireError as error:
                assert error.sqlstate == "40001", error
            else:
                raise AssertionError("index drop committed despite a concurrent view dependency")


def verify_view_sequence_portal_identity(port):
    with contextlib.closing(Connection(port)) as reader, contextlib.closing(Connection(port)) as writer:
        writer.query("CREATE SEQUENCE view_sequence_portal_source")
        writer.query("CREATE TABLE view_sequence_portal_rows (id int)")
        writer.query("INSERT INTO view_sequence_portal_rows VALUES (1), (2), (3), (4)")
        writer.query(
            "CREATE VIEW view_sequence_portal AS "
            "SELECT nextval('view_sequence_portal_source') AS number FROM view_sequence_portal_rows"
        )
        statement = b"view_sequence_portal_plan"
        assert cycle(reader, [parse("SELECT number FROM view_sequence_portal", (), name=statement)]) == []
        reader.socket.sendall(
            bind(statement=statement) + frame(b"E", struct.pack("!BI", 0, 1)) + frame(b"H", b"")
        )
        results = []
        while True:
            kind, body = reader.receive()
            assert kind not in (b"C", b"E"), (kind, body)
            if kind == b"s":
                break
            if kind == b"D":
                results.append(int(body[6:]))
        assert results == [1]
        writer.query("CREATE OR REPLACE VIEW view_sequence_portal AS SELECT 0::bigint AS number")
        writer.query("DROP SEQUENCE view_sequence_portal_source")
        writer.query("CREATE SEQUENCE view_sequence_portal_source START WITH 70")
        assert cycle(reader, [frame(b"E", bytes(5))]) == [(b"E", "42P01")]
        assert writer.query("SELECT nextval('view_sequence_portal_source')") == [["70"]]


def verify_view_replacement_contract(port):
    with contextlib.closing(Connection(port)) as reader, contextlib.closing(
        Connection(port)
    ) as writer:
        writer.query("CREATE TABLE replace_view_base (id int, label varchar(8))")
        writer.query("INSERT INTO replace_view_base VALUES (7, 'seven')")
        writer.query("CREATE VIEW replace_view AS SELECT * FROM replace_view_base")
        writer.query("ALTER TABLE replace_view_base ADD COLUMN extra int DEFAULT 11")
        writer.query("ALTER TABLE replace_view_base RENAME COLUMN id TO seed")
        assert reader.query("SELECT * FROM replace_view") == [["7", "seven"]]
        statement = b"replace_view_prepared"
        assert cycle(reader, [parse("SELECT id, label FROM replace_view", (), name=statement)]) == []

        def execute():
            return cycle(reader, [bind((), statement=statement), frame(b"E", bytes(5))])

        def row(value):
            return [(b"D", struct.pack("!Hi1si5s", 2, 1, value, 5, b"seven"))]

        assert execute() == row(b"7")
        for target in (
            "seed AS id",
            "seed, label",
            "seed::bigint AS id, label",
            "seed AS id, label::varchar(9) AS label",
        ):
            try:
                writer.query(
                    f"CREATE OR REPLACE VIEW replace_view AS SELECT {target} FROM replace_view_base"
                )
            except WireError as error:
                assert error.sqlstate == "42P16", error
            else:
                raise AssertionError(f"view replacement accepted incompatible output: {target}")
            assert execute() == row(b"7")

        writer.query(
            "CREATE OR REPLACE VIEW replace_view AS "
            "SELECT seed + 1 AS id, label, extra FROM replace_view_base"
        )
        assert reader.query("SELECT * FROM replace_view") == [["8", "seven", "11"]]
        assert execute() == row(b"8")
        writer.query("CREATE VIEW replace_view_child AS SELECT id FROM replace_view")
        try:
            writer.query("DROP VIEW replace_view RESTRICT")
        except WireError as error:
            assert error.sqlstate == "2BP01", error
        else:
            raise AssertionError("DROP VIEW RESTRICT removed a depended-on view")
        assert reader.query("SELECT id FROM replace_view_child") == [["8"]]
        writer.query("DROP VIEW replace_view CASCADE")
        absent(reader, "replace_view_child")
        absent(reader, "replace_view")


def verify_view_portal_during_replacement(port):
    with contextlib.closing(Connection(port)) as reader, contextlib.closing(
        Connection(port)
    ) as writer:
        writer.query("CREATE TABLE view_portal_base (bucket int, amount int)")
        writer.query("INSERT INTO view_portal_base VALUES (1, 1), (1, 2), (2, 3), (2, 4), (3, 5)")
        writer.query("CREATE TABLE view_portal_groups (bucket int)")
        writer.query("INSERT INTO view_portal_groups VALUES (1), (2), (3)")
        writer.query("CREATE VIEW view_portal_input AS SELECT bucket, amount FROM view_portal_base")
        writer.query("CREATE VIEW view_portal_nested AS SELECT * FROM view_portal_input")
        statement = b"view_portal_statement"
        sql = (
            "SELECT g.bucket, sum(v.amount) FROM view_portal_nested v "
            "JOIN view_portal_groups g ON v.bucket = g.bucket WHERE v.amount >= $1 "
            "GROUP BY g.bucket ORDER BY g.bucket LIMIT 2"
        )

        def row(bucket, amount):
            return struct.pack("!Hi1si", 2, 1, bucket, len(amount)) + amount

        def suspend(args):
            reader.socket.sendall(args + frame(b"E", struct.pack("!BI", 0, 1)) + frame(b"H", b""))
            rows = []
            while True:
                kind, body = reader.receive()
                assert kind not in (b"C", b"E"), (kind, body)
                if kind == b"s":
                    return rows
                if kind == b"D":
                    rows.append((kind, body))

        def replace(increment):
            writer.query(
                "CREATE OR REPLACE VIEW view_portal_input AS "
                f"SELECT bucket, amount + {increment} AS amount FROM view_portal_base"
            )

        assert cycle(reader, [parse(sql, (23,), name=statement)], with_message=True) == []
        args = bind((b"2",), statement=statement)
        writer.query("SET statement_timeout = '2s'")
        assert suspend(args) == [(b"D", row(b"1", b"2"))]
        replace(10)
        finish = frame(b"E", bytes(5))
        assert cycle(reader, [finish], with_message=True) == [(b"D", row(b"2", b"7"))]
        assert cycle(reader, [args, finish], with_message=True) == [
            (b"D", row(b"1", b"23")), (b"D", row(b"2", b"27")),
        ]

        stream = b"view_portal_stream"
        sql = "SELECT bucket, amount FROM view_portal_nested WHERE bucket = 2 AND amount >= $1"
        assert cycle(reader, [parse(sql, (23,), name=stream)], with_message=True) == []
        args = bind((b"12",), statement=stream)
        first = suspend(args)
        assert len(first) == 1
        replace(20)
        last = cycle(reader, [finish], with_message=True)
        assert len(last) == 1
        assert sorted(first + last) == [(b"D", row(b"2", b"13")), (b"D", row(b"2", b"14"))]
        assert sorted(cycle(reader, [args, finish], with_message=True)) == [
            (b"D", row(b"2", b"23")), (b"D", row(b"2", b"24")),
        ]


def verify_bounded_view_sort(port):
    with contextlib.closing(Connection(port)) as connection:
        connection.query("CREATE TABLE bounded_view_base (id int, payload varchar(127))")
        # Fill the heap with small rows, then admit wider rows that can force a spill.
        payloads = [f"{i:08d}" + "x" * (119 * (i % 2)) for i in range(320)]
        payloads[317] = "Ω"
        values = ", ".join(f"({i}, '{payloads[i]}')" for i in range(0, 320, 2))
        connection.query("INSERT INTO bounded_view_base VALUES " + values)
        values = ", ".join(f"({i}, '{payloads[i]}')" for i in range(319, 0, -2))
        connection.query("INSERT INTO bounded_view_base VALUES " + values + ", (-1, NULL)")
        connection.query("CREATE VIEW bounded_view_source AS SELECT * FROM bounded_view_base")
        connection.query(
            "CREATE VIEW bounded_view_result AS "
            "SELECT * FROM bounded_view_source ORDER BY payload DESC NULLS LAST LIMIT 64"
        )
        expected_ids = sorted(range(len(payloads)), key=payloads.__getitem__, reverse=True)[:64]
        expected = [[str(i)] for i in expected_ids]
        for work_mem in ("64kB", "4MB"):
            connection.query(f"SET work_mem = '{work_mem}'")
            assert connection.query(
                "SELECT id FROM bounded_view_result ORDER BY payload DESC NULLS LAST"
            ) == expected


def verify_view_crash_boundaries(server):
    with contextlib.closing(Connection(server.port)) as writer:
        writer.query("CREATE TABLE crash_view_base (id int, flag int)")
        writer.query("INSERT INTO crash_view_base VALUES (7, 1)")
        writer.query("CREATE VIEW crash_view AS SELECT id FROM crash_view_base WHERE flag = 1")
        writer.query("CREATE VIEW crash_view_nested AS SELECT id FROM crash_view")
        writer.query("ALTER TABLE crash_view_base RENAME COLUMN id TO renamed")
        writer.query("ALTER TABLE crash_view_base ADD COLUMN extra int")
        definition, = writer.query(
            "SELECT definition FROM pg_views WHERE viewname = 'crash_view'"
        )[0]
        assert "renamed" in definition
        assert writer.query(
            "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'crash_view'"
        ) == [[definition]]
        assert writer.query(
            f"SELECT id FROM ({definition.rstrip(';')}) AS reconstructed"
        ) == [["7"]]
    for point, expected in (
        ("before-terminal-append", "7"),
        ("after-catalog-transaction-wal-flush", "8"),
    ):
        with contextlib.closing(Connection(server.port)) as writer, contextlib.closing(
            Connection(server.port)
        ) as observer:
            writer.query("BEGIN")
            writer.query(
                "CREATE OR REPLACE VIEW crash_view AS "
                "SELECT renamed + 1 AS id FROM crash_view_base WHERE flag = 1"
            )
            assert writer.query("SELECT * FROM crash_view_nested") == [["8"]]
            assert observer.query("SELECT * FROM crash_view_nested") == [["7"]]
            crash_during_statement(server, writer, point, "COMMIT")
        server.start()
        with contextlib.closing(Connection(server.port)) as reader:
            assert reader.query("SELECT * FROM crash_view_nested") == [[expected]]
            assert reader.query("""
                SELECT relname, attname, attnum, atttypid, atttypmod
                FROM pg_attribute JOIN pg_class ON attrelid = oid
                WHERE relname IN ('crash_view', 'crash_view_nested') AND attnum > 0
                ORDER BY relname, attnum
            """) == [
                ["crash_view", "id", "1", "23", "-1"],
                ["crash_view_nested", "id", "1", "23", "-1"],
            ]
            recovered_sql, = reader.query(
                "SELECT definition FROM pg_views WHERE viewname = 'crash_view'"
            )[0]
            assert "renamed" in recovered_sql
            assert reader.query(
                "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'crash_view'"
            ) == [[recovered_sql]]
            assert reader.query(
                f"SELECT id FROM ({recovered_sql.rstrip(';')}) AS reconstructed"
            ) == [[expected]]
            views = reader.query(
                "SELECT oid, relkind FROM pg_class "
                "WHERE relname IN ('crash_view', 'crash_view_nested') ORDER BY relname"
            )
            assert len(views) == 2
            for oid, kind in views:
                assert kind == "v"
                assert not (server.data / "base" / oid).exists()
            try:
                reader.query("ALTER TABLE crash_view_base DROP COLUMN flag")
            except WireError as error:
                assert error.sqlstate == "2BP01", error
            else:
                raise AssertionError("recovered view lost its attribute dependency")


def verify_view_logical_ddl_rejected(server):
    with contextlib.closing(Connection(server.port)) as writer:
        writer.query("CREATE VIEW view_before_capture AS SELECT 1 AS id")
        writer.query("CREATE TABLE view_capture_table (id int)")
        try:
            writer.query("CREATE PUBLICATION view_capture_invalid FOR TABLE view_before_capture")
        except WireError as error:
            assert error.sqlstate == "22023", error
        else:
            raise AssertionError("an explicit publication accepted a view")
        writer.query("CREATE PUBLICATION view_capture_all FOR ALL TABLES")
        assert writer.query(
            "SELECT c.relname FROM pg_get_publication_tables('view_capture_all') p "
            "JOIN pg_class c ON c.oid = p.relid "
            "WHERE c.relname IN ('view_before_capture', 'view_capture_table')"
        ) == [["view_capture_table"]]
        bootstrap, = writer.query("SELECT pg_lisp_replication_bootstrap()")[0]
        assert "view_capture_table" in bootstrap
        assert "view_before_capture" not in bootstrap
        create_confirmed_transactional_slot(server, "view_capture")
        try:
            for statement in (
                "CREATE VIEW view_during_capture AS SELECT 2 AS id",
                "CREATE OR REPLACE VIEW view_before_capture AS SELECT 2 AS id",
                "DROP VIEW view_before_capture",
            ):
                try:
                    writer.query(statement)
                except WireError as error:
                    assert error.sqlstate == "0A000" and "view DDL" in str(error), error
                else:
                    raise AssertionError(f"publisher admitted unsupported logical DDL: {statement}")
            assert writer.query("SELECT id FROM view_before_capture") == [["1"]]
            absent(writer, "view_during_capture")
        finally:
            deadline = time.monotonic() + 10
            while writer.query(
                "SELECT active FROM pg_replication_slots WHERE slot_name='view_capture'"
            ) == [["t"]]:
                assert time.monotonic() < deadline, "view capture sender did not release the slot"
                time.sleep(0.05)
            writer.query("SELECT pg_drop_replication_slot('view_capture')")
            writer.query("DROP PUBLICATION view_capture_all")


def verify_joined_view_definition(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE view_join_left (id int)")
        connection.query("CREATE TABLE view_join_right (id int)")
        connection.query("INSERT INTO view_join_left VALUES (1), (2)")
        connection.query("INSERT INTO view_join_right VALUES (2), (3)")
        connection.query(
            "CREATE VIEW view_joined AS "
            "SELECT id FROM view_join_left FULL JOIN view_join_right USING (id)"
        )
        connection.query("ALTER TABLE view_join_right RENAME COLUMN id TO new_id")
        definition, = connection.query(
            "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'view_joined'"
        )[0]
        assert "new_id" in definition
        assert connection.query(
            f"SELECT id FROM ({definition.rstrip(';')}) AS reconstructed ORDER BY id"
        ) == connection.query("SELECT id FROM view_joined ORDER BY id") == [["1"], ["2"], ["3"]]


def verify_ordered_view_definition(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE view_order_base (id int, score int)")
        connection.query("INSERT INTO view_order_base VALUES (1, 20), (2, NULL), (3, 30), (4, 10)")
        connection.query(
            "CREATE VIEW view_ordered AS "
            "SELECT id FROM view_order_base ORDER BY score DESC NULLS LAST LIMIT 2"
        )
        connection.query(
            "CREATE VIEW view_distinct_ordered AS "
            "SELECT DISTINCT score AS points FROM view_order_base ORDER BY points NULLS FIRST LIMIT 2"
        )
        connection.query("ALTER TABLE view_order_base RENAME COLUMN score TO renamed_score")
        for view, expected in (("view_ordered", [["3"], ["1"]]),
                               ("view_distinct_ordered", [[None], ["10"]])):
            definition, = connection.query(
                f"SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = '{view}'"
            )[0]
            assert "renamed_score" in definition
            assert connection.query(definition) == connection.query(f"SELECT * FROM {view}") == expected


def verify_grouped_view_definition(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE view_group_base (amount int, category int)")
        connection.query("INSERT INTO view_group_base VALUES (2, 1), (3, 1), (8, 2), (9, 0)")
        connection.query(
            "CREATE VIEW view_grouped AS "
            "SELECT category, count(*) AS records, sum(amount) AS total "
            "FROM view_group_base WHERE amount > 0 GROUP BY category "
            "HAVING category > 0 AND sum(amount) > 3 ORDER BY total DESC"
        )
        connection.query("ALTER TABLE view_group_base RENAME COLUMN amount TO renamed_amount")
        connection.query("ALTER TABLE view_group_base RENAME COLUMN category TO renamed_category")
        definition, = connection.query(
            "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'view_grouped'"
        )[0]
        assert "renamed_amount" in definition and "renamed_category" in definition
        assert connection.query(definition) == connection.query("SELECT * FROM view_grouped") == [
            ["2", "1", "8"], ["1", "2", "5"]
        ]


def verify_union_view_definition(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE view_union_left (id int)")
        connection.query("CREATE TABLE view_union_right (id int)")
        connection.query("INSERT INTO view_union_left VALUES (1), (2)")
        connection.query("INSERT INTO view_union_right VALUES (2), (3)")
        connection.query(
            "CREATE VIEW view_union (value) AS "
            "(SELECT id FROM view_union_left ORDER BY id DESC LIMIT 1) "
            "UNION SELECT id FROM view_union_right UNION ALL SELECT '4' "
            "ORDER BY id DESC LIMIT 3"
        )
        connection.query("ALTER TABLE view_union_left RENAME COLUMN id TO left_id")
        connection.query("ALTER TABLE view_union_right RENAME COLUMN id TO right_id")
        definition, = connection.query(
            "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'view_union'"
        )[0]
        assert "left_id" in definition and "right_id" in definition
        assert connection.query(definition) == connection.query("SELECT value FROM view_union") == [
            ["4"], ["3"], ["2"]
        ]


def verify_cte_view_definition(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE view_cte_base (id int)")
        connection.query("INSERT INTO view_cte_base VALUES (1)")
        connection.query(
            "CREATE VIEW view_cte AS WITH src(value) AS MATERIALIZED "
            "(SELECT id FROM view_cte_base), step AS (SELECT value + 1 AS value FROM src) "
            "SELECT value FROM step"
        )
        connection.query(
            "CREATE VIEW view_recursive AS "
            "WITH RECURSIVE chain(value) AS (SELECT id FROM view_cte_base "
            "UNION ALL SELECT value + 1 FROM chain WHERE value < 3) "
            "SELECT value FROM chain ORDER BY value"
        )
        connection.query("ALTER TABLE view_cte_base RENAME COLUMN id TO seed")
        for view, expected, keyword in (
            ("view_cte", [["2"]], "MATERIALIZED"),
            ("view_recursive", [["1"], ["2"], ["3"]], "WITH RECURSIVE"),
        ):
            definition, = connection.query(
                f"SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = '{view}'"
            )[0]
            assert "seed" in definition and keyword in definition
            assert connection.query(definition) == connection.query(f"SELECT * FROM {view}") == expected


def verify_derived_view_definition(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE view_derived_base (id int)")
        connection.query("INSERT INTO view_derived_base VALUES (1), (2)")
        connection.query(
            "CREATE VIEW view_derived (first_value, second_value) AS SELECT d.* FROM "
            "(SELECT id AS repeated, id + 10 AS repeated "
            "FROM view_derived_base ORDER BY id DESC LIMIT 1) AS d"
        )
        connection.query("ALTER TABLE view_derived_base RENAME COLUMN id TO seed")
        definition, = connection.query(
            "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'view_derived'"
        )[0]
        assert "seed" in definition
        assert connection.query(definition) == connection.query("SELECT * FROM view_derived") == [["2", "12"]]


def verify_lateral_view_definition(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE view_lateral_outer (id int)")
        connection.query("CREATE TABLE view_lateral_inner (id int)")
        connection.query("INSERT INTO view_lateral_outer VALUES (1), (3)")
        connection.query("INSERT INTO view_lateral_inner VALUES (2), (4), (5)")
        connection.query(
            "CREATE VIEW view_lateral AS SELECT o.id, d.next_id "
            "FROM view_lateral_outer o CROSS JOIN LATERAL "
            "(SELECT i.id AS next_id FROM view_lateral_inner i WHERE i.id > o.id "
            "ORDER BY i.id LIMIT 1) d ORDER BY o.id"
        )
        connection.query("ALTER TABLE view_lateral_outer RENAME COLUMN id TO outer_id")
        connection.query("ALTER TABLE view_lateral_inner RENAME COLUMN id TO inner_id")
        definition, = connection.query(
            "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'view_lateral'"
        )[0]
        assert "outer_id" in definition and "inner_id" in definition and "LATERAL" in definition
        assert connection.query(definition) == connection.query("SELECT * FROM view_lateral") == [
            ["1", "2"], ["3", "4"]
        ]


def verify_sublink_view_definition(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE view_subquery_out (id int)")
        connection.query("CREATE TABLE view_subquery_in (id int)")
        connection.query("INSERT INTO view_subquery_out VALUES (1), (2), (4)")
        connection.query("INSERT INTO view_subquery_in VALUES (2), (4), (5)")
        connection.query(
            "CREATE VIEW view_subquery AS SELECT o.id, "
            "(SELECT min(i.id) FROM view_subquery_in i WHERE i.id > o.id) AS next_id "
            "FROM view_subquery_out o WHERE EXISTS "
            "(SELECT 1 FROM view_subquery_in i WHERE i.id > o.id) "
            "AND o.id = ANY (SELECT i.id FROM view_subquery_in i) ORDER BY o.id"
        )
        connection.query("ALTER TABLE view_subquery_out RENAME COLUMN id TO outer_id")
        connection.query("ALTER TABLE view_subquery_in RENAME COLUMN id TO inner_id")
        definition, = connection.query(
            "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'view_subquery'"
        )[0]
        assert all(term in definition for term in ("outer_id", "inner_id", "EXISTS", "ANY"))
        assert connection.query(definition) == connection.query("SELECT * FROM view_subquery") == [
            ["2", "4"], ["4", "5"]
        ]


def verify_window_view_definition(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE view_window_base (grp int, val int)")
        connection.query("INSERT INTO view_window_base VALUES (1, 2), (1, 3), (2, 1)")
        connection.query(
            "CREATE VIEW view_window AS SELECT grp, val, row_number() OVER window_keys AS pos, "
            "sum(val) OVER (window_keys ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total "
            "FROM view_window_base WINDOW window_keys AS (PARTITION BY grp ORDER BY val) "
            "ORDER BY grp, pos"
        )
        connection.query("ALTER TABLE view_window_base RENAME COLUMN grp TO category")
        connection.query("ALTER TABLE view_window_base RENAME COLUMN val TO amount")
        definition, = connection.query(
            "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'view_window'"
        )[0]
        assert all(term in definition for term in ("category", "amount", "ROWS BETWEEN"))
        assert connection.query(definition) == connection.query("SELECT * FROM view_window") == [
            ["1", "2", "1", "2"], ["1", "3", "2", "5"], ["2", "1", "1", "1"]
        ]


def verify_set_returning_view_definition(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE view_series_base (n int)")
        connection.query("INSERT INTO view_series_base VALUES (pg_backend_pid())")
        pid, = connection.query("SELECT n FROM view_series_base")[0]
        connection.query(
            "CREATE VIEW view_series AS SELECT n, (pg_stat_get_activity(n)).pid AS pid "
            "FROM view_series_base"
        )
        connection.query(
            "CREATE VIEW view_series_from AS SELECT b.n, activity.pid "
            "FROM view_series_base b CROSS JOIN pg_stat_get_activity(b.n) AS activity"
        )
        connection.query("ALTER TABLE view_series_base RENAME COLUMN n TO bound")
        for view in ("view_series", "view_series_from"):
            definition, = connection.query(
                f"SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = '{view}'"
            )[0]
            assert "bound" in definition and "pg_stat_get_activity" in definition
            assert connection.query(definition) == connection.query(f"SELECT * FROM {view}") == [
                [pid, pid]
            ]


def verify_variadic_view_definition(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE view_published (id int)")
        connection.query("CREATE PUBLICATION view_variadic_pub FOR TABLE view_published")
        connection.query("CREATE TABLE view_publications (pubname text)")
        connection.query("INSERT INTO view_publications VALUES ('view_variadic_pub')")
        connection.query(
            "CREATE VIEW view_variadic AS SELECT p.relid FROM view_publications v "
            "CROSS JOIN pg_get_publication_tables(v.pubname) AS p"
        )
        connection.query("ALTER TABLE view_publications RENAME COLUMN pubname TO publication")
        definition, = connection.query(
            "SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'view_variadic'"
        )[0]
        expected = connection.query("SELECT oid FROM pg_class WHERE relname = 'view_published'")
        assert "publication" in definition and "VARIADIC" in definition
        assert connection.query(definition) == connection.query("SELECT * FROM view_variadic") == expected


def verify_view_definition_by_name(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query('CREATE VIEW public."Mixed.View" AS SELECT 17 AS id')
        qualified, unqualified, oid = connection.query("""
            SELECT pg_get_viewdef('public."Mixed.View"'),
                   pg_get_viewdef('"Mixed.View"', true), pg_get_viewdef(oid)
            FROM pg_class WHERE relname = 'Mixed.View'
        """)[0]
        assert qualified == unqualified == oid
        assert connection.query(qualified) == [["17"]]
        try:
            connection.query("SELECT pg_get_viewdef('missing_view')")
        except WireError as error:
            assert error.sqlstate == "42P01", error
        else:
            raise AssertionError("pg_get_viewdef resolved a missing relation")


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



def verify_scalar_sql_routine_transactions(port):
    definition = (
        "CREATE FUNCTION p12_sum(x integer, y integer DEFAULT 4) RETURNS integer "
        "LANGUAGE sql IMMUTABLE STRICT AS $$SELECT 1; SELECT x + p12_sum.y$$"
    )
    with contextlib.closing(Connection(port)) as writer, contextlib.closing(Connection(port)) as reader:
        writer.query("BEGIN")
        writer.query(definition)
        assert writer.query("SELECT p12_sum(3), public.p12_sum(5,6), p12_sum(NULL::integer)") == [
            ["7", "11", None]
        ]
        assert writer.query("SELECT p12_sum(y => 6, x => 5), p12_sum(x => 3)") == [["11", "7"]]
        try:
            reader.query("SELECT p12_sum(3)")
        except WireError as error:
            assert error.sqlstate == "42883", error
        else:
            raise AssertionError("uncommitted routine was visible to another connection")
        writer.query("COMMIT")
        assert reader.query("SELECT p12_sum(3)") == [["7"]]

        writer.query("BEGIN")
        writer.query(definition.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION")
                     .replace("x + p12_sum.y", "$1 * $2"))
        assert writer.query("SELECT p12_sum(3)") == [["12"]]
        assert reader.query("SELECT p12_sum(3)") == [["7"]]
        writer.query("ROLLBACK")
        assert writer.query("SELECT p12_sum(3)") == [["7"]]

        prepared = b"p12_routine_generation"
        assert cycle(reader, [parse("SELECT p12_sum($1)", (), name=prepared)]) == []

        def prepared_result():
            return cycle(reader, [bind((b"3",), statement=prepared), frame(b"E", bytes(5))])

        assert prepared_result() == [(b"D", struct.pack("!Hi1s", 1, 1, b"7"))]
        writer.query(definition.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION")
                     .replace("x + p12_sum.y", "$1 * $2"))
        assert prepared_result() == [(b"D", struct.pack("!Hi2s", 1, 2, b"12"))]
        writer.query(definition.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION"))

        for body, sqlstate in (("SELECT NULL", "42P13"), ("SELECT missing_column", "42703")):
            invalid = definition.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION").replace(
                "SELECT 1; SELECT x + p12_sum.y", body
            )
            try:
                writer.query(invalid)
            except WireError as error:
                assert error.sqlstate == sqlstate, error
            else:
                raise AssertionError("invalid SQL routine definition was accepted")
            assert writer.query("SELECT p12_sum(3)") == [["7"]]

        writer.query("SET check_function_bodies = off")
        writer.query(
            "CREATE FUNCTION p12_forward() RETURNS integer LANGUAGE sql "
            "AS $$SELECT p12_later()$$"
        )
        writer.query("RESET check_function_bodies")
        assert writer.query("SHOW check_function_bodies") == [["on"]]
        writer.query("CREATE FUNCTION p12_later() RETURNS integer LANGUAGE sql AS $$SELECT 12$$")
        assert writer.query("SELECT p12_forward()") == [["12"]]
        writer.query("DROP FUNCTION p12_forward(), p12_later()")

        writer.query(
            "CREATE FUNCTION p12_overload(integer) RETURNS integer LANGUAGE sql AS $$SELECT 4$$; "
            "CREATE FUNCTION p12_overload(bigint) RETURNS integer LANGUAGE sql AS $$SELECT 8$$"
        )
        assert writer.query("SELECT p12_overload(1), p12_overload(1::bigint)") == [["4", "8"]]
        for sql in ("SELECT p12_overload(NULL)", "DROP FUNCTION p12_overload"):
            try:
                writer.query(sql)
            except WireError as error:
                assert error.sqlstate == "42725", error
            else:
                raise AssertionError("ambiguous routine signature was accepted")
        writer.query("DROP FUNCTION p12_overload(integer)")
        assert writer.query("SELECT p12_overload(1)") == [["8"]]
        writer.query("DROP FUNCTION p12_overload(bigint)")


def verify_recovered_scalar_sql_routine(port):
    with contextlib.closing(Connection(port)) as connection:
        assert connection.query("SELECT p12_sum(3)") == [["7"]]
        connection.query("DROP FUNCTION public.p12_sum(integer, integer)")
        try:
            connection.query("SELECT p12_sum(3)")
        except WireError as error:
            assert error.sqlstate == "42883", error
        else:
            raise AssertionError("dropped routine remains callable")


def create_durable_routine_expressions(port):
    with contextlib.closing(Connection(port)) as connection:
        connection.query(
            "CREATE FUNCTION p12_expr_default() RETURNS integer LANGUAGE plpgsql "
            "AS $$BEGIN RETURN 7; END$$"
        )
        connection.query(
            "CREATE FUNCTION p12_expr_check(x integer) RETURNS boolean LANGUAGE plpgsql IMMUTABLE "
            "AS $$BEGIN RETURN x > 0; END$$"
        )
        connection.query(
            "CREATE TABLE p12_expressions (x integer DEFAULT p12_expr_default(), "
            "CONSTRAINT p12_positive CHECK (p12_expr_check(x)))"
        )
        assert connection.query("INSERT INTO p12_expressions DEFAULT VALUES RETURNING x") == [["7"]]


def verify_recovered_routine_expressions(port):
    with contextlib.closing(Connection(port)) as connection:
        assert connection.query("SELECT x FROM p12_expressions") == [["7"]]
        for sql, sqlstate in (
            ("DROP FUNCTION p12_expr_default()", "2BP01"),
            ("DROP FUNCTION p12_expr_check(integer)", "2BP01"),
            ("INSERT INTO p12_expressions VALUES (-1)", "23514"),
        ):
            try:
                connection.query(sql)
            except WireError as error:
                assert error.sqlstate == sqlstate, error
            else:
                raise AssertionError(f"expected stored routine dependency to reject {sql}")
        connection.query(
            "CREATE OR REPLACE FUNCTION p12_expr_default() RETURNS integer LANGUAGE plpgsql "
            "AS $$BEGIN RETURN 9; END$$"
        )
        assert connection.query("INSERT INTO p12_expressions DEFAULT VALUES RETURNING x") == [["9"]]

        drop = "DROP FUNCTION p12_expr_default(), p12_expr_check(integer) CASCADE"
        connection.query("BEGIN")
        connection.query(drop)
        assert connection.query("INSERT INTO p12_expressions DEFAULT VALUES RETURNING x") == [[None]]
        connection.query("INSERT INTO p12_expressions VALUES (-1)")
        connection.query("ROLLBACK")
        assert connection.query("INSERT INTO p12_expressions DEFAULT VALUES RETURNING x") == [["9"]]

        connection.query(drop)
        assert connection.query("INSERT INTO p12_expressions DEFAULT VALUES RETURNING x") == [[None]]
        connection.query("INSERT INTO p12_expressions VALUES (-1)")
        assert connection.query("SELECT x FROM p12_expressions ORDER BY x") == [
            ["-1"], ["7"], ["9"], ["9"], [None]
        ]
        connection.query("DROP TABLE p12_expressions")


def create_durable_plpgsql_routine(port):
    with contextlib.closing(Connection(port)) as connection:
        connection.query(
            "CREATE FUNCTION p12_durable_plpgsql(n integer) RETURNS integer LANGUAGE plpgsql STRICT "
            "AS $$DECLARE doubled integer := n * 2; BEGIN RETURN doubled; END$$"
        )


def verify_recovered_plpgsql_routine(port):
    with contextlib.closing(Connection(port)) as connection:
        assert connection.query(
            "SELECT p12_durable_plpgsql(3), p12_durable_plpgsql(NULL::integer)"
        ) == [["6", None]]
        connection.query("DROP FUNCTION p12_durable_plpgsql(integer)")


def verify_sql_routine_dml(port):
    with contextlib.closing(Connection(port)) as connection:
        connection.query("CREATE TABLE p12_changes (id integer PRIMARY KEY, value integer)")
        connection.query(
            "CREATE FUNCTION p12_change(n integer) RETURNS integer LANGUAGE sql AS "
            "$$INSERT INTO p12_changes VALUES (n, 1); "
            "UPDATE p12_changes SET value = value + 1 WHERE id = n RETURNING value$$"
        )
        connection.query(
            "CREATE FUNCTION p12_remove(n integer) RETURNS bigint LANGUAGE sql AS "
            "$$DELETE FROM p12_changes WHERE id = n; SELECT count(*) FROM p12_changes$$"
        )
        assert connection.query("SELECT * FROM p12_changes") == []
        connection.query("BEGIN")
        assert connection.query("SELECT p12_change(7)") == [["2"]]
        assert connection.query("SELECT * FROM p12_changes") == [["7", "2"]]
        connection.query("ROLLBACK")
        assert connection.query("SELECT * FROM p12_changes") == []
        for key in (1, 2):
            assert connection.query(f"SELECT p12_change({key})") == [["2"]]
        assert connection.query("SELECT p12_remove(1)") == [["1"]]
        assert connection.query("SELECT * FROM p12_changes") == [["2", "2"]]

        connection.query("BEGIN READ ONLY")
        try:
            connection.query("SELECT p12_change(9)")
        except WireError as error:
            assert error.sqlstate == "25006", error
        else:
            raise AssertionError("routine bypassed transaction read-only mode")
        connection.query("ROLLBACK")
        assert connection.query("SELECT * FROM p12_changes") == [["2", "2"]]

        connection.query(
            "CREATE FUNCTION p12_fail() RETURNS integer LANGUAGE sql AS "
            "$$INSERT INTO p12_changes VALUES (8, 1); "
            "INSERT INTO p12_changes VALUES (8, 2); SELECT 1$$"
        )
        try:
            connection.query("SELECT p12_fail()")
        except WireError as error:
            assert error.sqlstate == "23505", error
        else:
            raise AssertionError("expected unique violation from routine body")
        assert connection.query("SELECT * FROM p12_changes") == [["2", "2"]]

        connection.query(
            "CREATE FUNCTION p12_stable_write() RETURNS integer LANGUAGE sql STABLE AS "
            "$$INSERT INTO p12_changes VALUES (9, 1); SELECT 1$$"
        )
        try:
            connection.query("SELECT p12_stable_write()")
        except WireError as error:
            assert error.sqlstate == "0A000", error
        else:
            raise AssertionError("STABLE routine performed a write")
        assert connection.query("SELECT * FROM p12_changes") == [["2", "2"]]

        connection.query(
            "CREATE FUNCTION p12_drain() RETURNS integer LANGUAGE sql AS "
            "$$DELETE FROM p12_changes RETURNING value$$"
        )
        assert connection.query("SELECT p12_change(3)") == [["2"]]
        assert connection.query("SELECT p12_drain()") == [["2"]]
        assert connection.query("SELECT * FROM p12_changes") == []
        assert connection.query("SELECT p12_drain()") == [[None]]
        connection.query(
            "DROP FUNCTION p12_change(integer), p12_remove(integer), p12_fail(), "
            "p12_stable_write(), p12_drain()"
        )
        connection.query("DROP TABLE p12_changes")


def verify_sql_set_portal_snapshots(port, reader):
    with contextlib.closing(Connection(port)) as writer:
        for volatility in ("STABLE", "VOLATILE"):
            writer.query(
                f"CREATE FUNCTION p12_pinned() RETURNS SETOF integer LANGUAGE sql {volatility} "
                "AS $$SELECT id FROM p12_set_input WHERE id >= 102 ORDER BY id$$"
            )
            reader.query("BEGIN")
            execute_one = frame(b"E", b"\0" + struct.pack("!I", 1))
            assert cycle(reader, [parse("SELECT p12_pinned()"), bind(), execute_one]) == [
                (b"D", struct.pack("!Hi3s", 1, 3, b"102"))
            ]
            writer.query("INSERT INTO p12_set_input VALUES (999)")
            assert cycle(reader, [frame(b"E", bytes(5))]) == [
                (b"D", struct.pack("!Hi3s", 1, 3, b"103"))
            ]
            reader.query("COMMIT")
            writer.query("DELETE FROM p12_set_input WHERE id = 999")
            writer.query("DROP FUNCTION p12_pinned()")


def verify_sql_set_routines(port, *, require_index=True):
    with contextlib.closing(Connection(port)) as connection:
        connection.query("CREATE TABLE p12_set_input (id integer PRIMARY KEY)")
        connection.query("INSERT INTO p12_set_input VALUES (1),(2),(3)")
        connection.query(
            "CREATE FUNCTION p12_numbers(n integer) RETURNS SETOF bigint LANGUAGE sql STABLE STRICT "
            "AS $$SELECT id FROM p12_set_input WHERE id >= n ORDER BY id$$"
        )
        connection.query(
            "CREATE FUNCTION p12_records(n integer) RETURNS TABLE(output_id bigint, label text) "
            "LANGUAGE sql STABLE AS $$SELECT id, id::text AS item FROM p12_set_input "
            "WHERE id >= n ORDER BY id$$"
        )
        connection.query(
            "CREATE FUNCTION p12_one() RETURNS TABLE(output_id integer) "
            "LANGUAGE sql STABLE AS $$SELECT 7$$"
        )
        assert connection.query("SELECT p12_numbers(2)") == [["2"], ["3"]]
        assert connection.query("SELECT * FROM p12_numbers(NULL::integer)") == []
        assert connection.query("SELECT * FROM p12_records(2)") == [["2", "2"], ["3", "3"]]
        assert connection.query("SELECT p12_records(2)") == [["(2,2)"], ["(3,3)"]]
        assert connection.query("SELECT output_id FROM p12_one() AS renamed") == [["7"]]
        connection.query(
            "CREATE FUNCTION p12_set_subquery() RETURNS SETOF integer LANGUAGE sql STABLE "
            "AS $$SELECT (SELECT id FROM p12_set_input WHERE id = 1); SELECT 11$$"
        )
        assert connection.query("SELECT p12_set_subquery()") == [["11"]]

        connection.query("CREATE SEQUENCE p12_set_seq")
        connection.query(
            "CREATE FUNCTION p12_effects() RETURNS SETOF bigint LANGUAGE sql VOLATILE AS "
            "$$SELECT nextval('p12_set_seq') FROM p12_set_input$$"
        )
        connection.query("BEGIN")
        execute_one = frame(b"E", b"\0" + struct.pack("!I", 1))
        assert cycle(connection, [parse("SELECT p12_effects()"), bind(), execute_one]) == [
            (b"D", struct.pack("!Hi1s", 1, 1, b"1"))
        ]
        assert cycle(connection, [execute_one]) == [(b"D", struct.pack("!Hi1s", 1, 1, b"2"))]
        assert cycle(connection, [frame(b"C", b"P\0")]) == []
        assert connection.query("SELECT currval('p12_set_seq')") == [["2"]]
        connection.query("ROLLBACK")
        assert connection.query("SELECT p12_effects() LIMIT 1") == [["3"]]
        assert connection.query("SELECT currval('p12_set_seq')") == [["3"]]
        assert connection.query("SELECT * FROM p12_effects() LIMIT 1") == [["4"]]
        assert connection.query("SELECT currval('p12_set_seq')") == [["6"]]

        rows = ",".join(f"({value})" for value in range(4, 104))
        connection.query("INSERT INTO p12_set_input VALUES " + rows)
        suffix = "x" * 1024
        connection.query(
            "CREATE FUNCTION p12_wide_set() RETURNS SETOF text LANGUAGE sql VOLATILE "
            f"AS $$SELECT id::text || '{suffix}' FROM p12_set_input$$"
        )
        connection.query("SET work_mem = '64kB'")
        assert connection.query("SELECT count(*) FROM p12_wide_set()") == [["103"]]
        connection.query("RESET work_mem")

        sources = " CROSS JOIN ".join(f"p12_set_input AS {alias}" for alias in "abcde")
        connection.query(
            "CREATE FUNCTION p12_cancel_set() RETURNS SETOF bigint LANGUAGE sql STABLE "
            f"AS $$SELECT sum(a.id::bigint + e.id) FROM {sources}$$"
        )
        connection.query("SET statement_timeout = '30ms'")
        try:
            connection.query("SELECT p12_cancel_set()")
        except WireError as error:
            assert error.sqlstate == "57014", error
        else:
            raise AssertionError("expected cancellation during a routine set pull")
        connection.query("RESET statement_timeout")
        assert connection.query("SELECT p12_numbers(103)") == [["103"]]

        verify_sql_set_portal_snapshots(port, connection)
        for start in range(-3000, 0, 300):
            padding = ",".join(f"({value})" for value in range(start, start + 300))
            connection.query("INSERT INTO p12_set_input VALUES " + padding)
        connection.query("ANALYZE p12_set_input")
        connection.query("CREATE TABLE p12_set_outer (id integer PRIMARY KEY)")
        connection.query("INSERT INTO p12_set_outer SELECT id FROM p12_set_input")
        connection.query("ANALYZE p12_set_outer")
        connection.query("BEGIN")
        indexed = "SELECT p12_numbers(o.id) FROM p12_set_outer AS o WHERE o.id = 2 LIMIT 1"
        plan = connection.query("EXPLAIN (COSTS OFF) " + indexed)
        if require_index:
            assert any("Index" in line[0] for line in plan), plan
        assert cycle(connection, [parse(indexed), bind(), execute_one]) == [
            (b"D", struct.pack("!Hi1s", 1, 1, b"2"))
        ]
        assert cycle(connection, [execute_one]) == []
        assert cycle(connection, [frame(b"C", b"P\0")]) == []
        connection.query("TRUNCATE p12_set_input")
        connection.query("ROLLBACK")
        connection.query("DROP TABLE p12_set_outer")

        connection.query(
            "CREATE FUNCTION p12_index_error(n integer) RETURNS SETOF integer LANGUAGE sql STABLE "
            "AS $$SELECT 10 / (id - n) FROM p12_set_input WHERE id = n$$"
        )
        plan = connection.query(
            "EXPLAIN (COSTS OFF) SELECT 10 / (id - 2) FROM p12_set_input WHERE id = 2"
        )
        if require_index:
            assert any("Index" in line[0] for line in plan), plan
        try:
            connection.query("SELECT p12_index_error(2)")
        except WireError as error:
            assert error.sqlstate == "22012", error
        else:
            raise AssertionError("expected the routine body division error")
        assert connection.query("SELECT p12_numbers(103)") == [["103"]]

        connection.query(
            "DROP FUNCTION p12_numbers(integer), p12_records(integer), p12_one(), p12_effects(), "
            "p12_cancel_set(), p12_set_subquery(), p12_index_error(integer), p12_wide_set()"
        )
        connection.query("DROP SEQUENCE p12_set_seq")
        connection.query("DROP TABLE p12_set_input")


def verify_plpgsql_scalar_routines(port):
    with contextlib.closing(Connection(port)) as connection:
        connection.query(
            "DO $$DECLARE v integer := 1; BEGIN WHILE v < 3 LOOP "
            "v := v + 1; END LOOP; IF v <> 3 THEN RAISE 'unexpected value %', v; END IF; END$$"
        )
        connection.query(
            "CREATE FUNCTION p12_plpgsql(n integer) RETURNS integer LANGUAGE plpgsql AS "
            "$$DECLARE x integer := n; BEGIN LOOP x := x + 1; EXIT WHEN x >= 3; END LOOP; "
            "PERFORM 1 WHERE x = 3; IF FOUND THEN SELECT 4 INTO STRICT x; "
            "ELSIF x > 3 THEN x := x + 5; ELSE RAISE 'not found'; END IF; RETURN x; END$$"
        )
        assert connection.query("SELECT p12_plpgsql(1), p12_plpgsql(7)") == [["4", "13"]]
        connection.query(
            "CREATE FUNCTION p12_plpgsql_dynamic(n integer) RETURNS integer LANGUAGE plpgsql AS "
            "$$BEGIN PERFORM 1 WHERE false; EXECUTE 'SELECT $1 + 1' INTO n USING n; "
            "IF FOUND THEN RAISE 'EXECUTE changed FOUND'; END IF; RETURN $1; END$$"
        )
        assert connection.query("SELECT p12_plpgsql_dynamic(7)") == [["8"]]
        connection.query(
            "CREATE FUNCTION p12_plpgsql_qualified(n integer) RETURNS integer LANGUAGE plpgsql AS "
            "$$DECLARE n integer := 1; BEGIN RETURN n + p12_plpgsql_qualified.n; END$$"
        )
        assert connection.query("SELECT p12_plpgsql_qualified(7)") == [["8"]]
        connection.query("CREATE TABLE p12_plpgsql_values (n integer)")
        connection.query("INSERT INTO p12_plpgsql_values VALUES (1),(2),(3)")
        connection.query(
            "CREATE FUNCTION p12_plpgsql_subquery() RETURNS bigint LANGUAGE plpgsql STABLE AS "
            "$$DECLARE result bigint := (SELECT count(*) FROM p12_plpgsql_values); "
            "BEGIN RETURN result + (SELECT max(n) FROM p12_plpgsql_values); END$$"
        )
        assert connection.query("SELECT p12_plpgsql_subquery()") == [["6"]]
        connection.query("CREATE SEQUENCE p12_plpgsql_seq")
        connection.query(
            "DO $$DECLARE first_value bigint; BEGIN EXECUTE "
            "'SELECT nextval(''p12_plpgsql_seq'') FROM p12_plpgsql_values' INTO first_value; "
            "IF first_value <> 1 THEN RAISE 'wrong first value'; END IF; END$$"
        )
        assert connection.query("SELECT currval('p12_plpgsql_seq')") == [["3"]]
        for sql, sqlstate in (
            ("DO $$DECLARE n integer; BEGIN n := true; END$$", "22P02"),
            ("DO $$BEGIN EXECUTE 'SELEC' USING 1 / 0; END$$", "22012"),
            (
                "CREATE FUNCTION p12_invalid() RETURNS integer LANGUAGE plpgsql AS "
                "$$BEGIN IF false THEN missing := 1; END IF; RETURN 2; END$$",
                "42601",
            ),
            (
                "CREATE FUNCTION p12_invalid() RETURNS integer LANGUAGE plpgsql AS $$BEGIN RETURN; END$$",
                "42601",
            ),
        ):
            try:
                connection.query(sql)
            except WireError as error:
                assert error.sqlstate == sqlstate, error
            else:
                raise AssertionError(f"expected PL/pgSQL to reject {sql}")
        connection.query(
            "CREATE FUNCTION p12_plpgsql_raise(n integer) RETURNS integer LANGUAGE plpgsql AS "
            "$$BEGIN RAISE SQLSTATE '22012' USING MESSAGE = 'value ' || n::text; END$$"
        )
        try:
            connection.query("SELECT p12_plpgsql_raise(7)")
        except WireError as error:
            assert error.sqlstate == "22012" and "value 7" in str(error), error
        else:
            raise AssertionError("expected the PL/pgSQL error")
        assert connection.query("SELECT p12_plpgsql_dynamic(8)") == [["9"]]
        connection.query(
            "DROP FUNCTION p12_plpgsql(integer), p12_plpgsql_dynamic(integer), "
            "p12_plpgsql_raise(integer), p12_plpgsql_subquery(), p12_plpgsql_qualified(integer)"
        )
        connection.query("DROP SEQUENCE p12_plpgsql_seq")
        connection.query("DROP TABLE p12_plpgsql_values")


def verify_plpgsql_exception_routines(port):
    with contextlib.closing(Connection(port)) as connection:
        connection.query("CREATE TABLE p12_exc_rows (id integer PRIMARY KEY)")
        connection.query(
            "CREATE FUNCTION p12_exception() RETURNS text LANGUAGE plpgsql AS $$"
            "DECLARE caught text; original text; BEGIN "
            "BEGIN EXECUTE 'INSERT INTO p12_exc_rows VALUES ($1)' USING 1; "
            "EXECUTE 'INSERT INTO p12_exc_rows VALUES ($1)' USING 1; "
            "EXCEPTION WHEN integrity_constraint_violation THEN "
            "GET STACKED DIAGNOSTICS caught = RETURNED_SQLSTATE, original = MESSAGE_TEXT; "
            "IF caught <> SQLSTATE OR original <> SQLERRM THEN RAISE 'wrong diagnostics'; END IF; "
            "END; EXECUTE 'INSERT INTO p12_exc_rows VALUES ($1)' USING 2; RETURN caught; END$$"
        )
        connection.query("BEGIN")
        assert connection.query("SELECT p12_exception()") == [["23505"]]
        assert connection.query("SELECT id FROM p12_exc_rows") == [["2"]]
        connection.query("ROLLBACK")
        assert connection.query("SELECT id FROM p12_exc_rows") == []

        assert connection.query("SELECT p12_exception()") == [["23505"]]
        connection.query(
            "CREATE FUNCTION p12_nested_exception() RETURNS text LANGUAGE plpgsql AS $$"
            "BEGIN BEGIN BEGIN EXECUTE 'INSERT INTO p12_exc_rows VALUES ($1)' USING 3; "
            "RAISE SQLSTATE '22012' USING MESSAGE = 'inner'; "
            "EXCEPTION WHEN division_by_zero THEN RAISE; END; "
            "EXCEPTION WHEN SQLSTATE '22000' THEN RETURN SQLSTATE || ':' || SQLERRM; END; "
            "RETURN 'missing'; END$$"
        )
        assert connection.query("SELECT p12_nested_exception()") == [["22012:inner"]]
        assert connection.query("SELECT id FROM p12_exc_rows") == [["2"]]
        connection.query(
            "CREATE FUNCTION p12_found() RETURNS boolean LANGUAGE plpgsql AS $$"
            "DECLARE result integer; BEGIN FOUND := true; "
            "BEGIN SELECT 1 INTO STRICT result WHERE false; "
            "EXCEPTION WHEN no_data_found THEN RETURN FOUND; END; RETURN true; END$$"
        )
        assert connection.query("SELECT p12_found()") == [["f"]]
        try:
            connection.query(
                "DO $$BEGIN RAISE 'caught'; EXCEPTION WHEN OTHERS THEN SQLSTATE := '00000'; END$$"
            )
        except WireError as error:
            assert error.sqlstate == "22005", error
        else:
            raise AssertionError("exception diagnostic variable was assignable")
        connection.query("DROP FUNCTION p12_exception(), p12_nested_exception(), p12_found()")
        connection.query("DROP TABLE p12_exc_rows")


def main():
    server = OwnedLisp()
    try:
        server.start()
        verify_scalar_sql_routine_transactions(server.port)
        create_durable_plpgsql_routine(server.port)
        create_durable_routine_expressions(server.port)
        server.stop(immediate=True)
        server.start()
        verify_recovered_scalar_sql_routine(server.port)
        verify_recovered_plpgsql_routine(server.port)
        verify_recovered_routine_expressions(server.port)
        verify_sql_routine_dml(server.port)
        verify_sql_set_routines(server.port)
        verify_plpgsql_scalar_routines(server.port)
        verify_plpgsql_exception_routines(server.port)
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
        verify_view_logical_ddl_rejected(server)
        verify_view_transaction_visibility(server.port)
        verify_view_read_only_and_acyclic(server.port)
        verify_view_definition_context(server.port, create=True)
        verify_whole_row_view_definition(server.port, create=True)
        verify_view_sequence_dependencies(server.port, create=True, race=True)
        verify_view_nonsequence_dependencies(server.port, create=True, race=True)
        verify_view_sequence_portal_identity(server.port)
        verify_view_replacement_contract(server.port)
        verify_view_portal_during_replacement(server.port)
        verify_bounded_view_sort(server.port)
        verify_ordered_catalog_capture(server)
        transactional_birth = verify_transactional_slot_handshake(server)
        server.stop(immediate=True)
        server.start()
        verify_catalog_capture_records(server)
        verify_view_definition_context(server.port)
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
        verify_view_crash_boundaries(server)
        verify_whole_row_view_definition(server.port)
        verify_view_sequence_dependencies(server.port)
        verify_view_nonsequence_dependencies(server.port)
        verify_joined_view_definition(server)
        verify_ordered_view_definition(server)
        verify_grouped_view_definition(server)
        verify_union_view_definition(server)
        verify_cte_view_definition(server)
        verify_derived_view_definition(server)
        verify_lateral_view_definition(server)
        verify_sublink_view_definition(server)
        verify_window_view_definition(server)
        verify_set_returning_view_definition(server)
        verify_variadic_view_definition(server)
        verify_view_definition_by_name(server)
        verify_logical_column_type_rewrite()
    finally:
        server.stop()
        shutil.rmtree(server.root)


if __name__ == "__main__":
    main()
