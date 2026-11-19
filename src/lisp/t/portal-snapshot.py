"""TX-03 regression: Parse establishes RR and Bind retains SELECT visibility.

Each completed protocol reply is the barrier for the other SQL session.
The disposable server and wire client come from command-complete.py.
"""
import importlib.util
from pathlib import Path
import struct
import time

spec = importlib.util.spec_from_file_location(
    "command_complete", Path(__file__).with_name("command-complete.py"))
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)


class Connection(wire.Connection):
    def send(self, kind, body=b""):
        super().send(kind, body)

    def through(self, expected):
        while True:
            kind, body = self.receive()
            assert kind != b"E", self.error_fields(body)
            if kind == expected:
                return body

    def prepare(self, table, bind=True, statement="stmt", portal="portal"):
        self.send(b"P", statement.encode() + b"\0SELECT id FROM "
                  + table.encode() + b"\0" + struct.pack("!H", 0))
        if bind:
            self.bind(statement, portal)
        self.send(b"H")
        self.through(b"2" if bind else b"1")

    def bind(self, statement="stmt", portal="portal"):
        self.send(b"B", portal.encode() + b"\0" + statement.encode()
                  + b"\0" + struct.pack("!HHH", 0, 0, 0))

    def execute(self, portal="portal"):
        self.send(b"E", portal.encode() + b"\0" + struct.pack("!I", 0))
        self.send(b"S")
        rows, commands, errors = [], [], []
        while True:
            kind, body = self.receive()
            if kind == b"D":
                rows.append(self.data_row(body))
            elif kind == b"C":
                commands.append(body.rstrip(b"\0").decode())
            elif kind == b"E":
                errors.append(self.error_fields(body))
            elif kind == b"Z":
                return rows, commands, body, errors

    def close_portal(self, portal="portal"):
        self.send(b"C", b"P" + portal.encode() + b"\0")
        self.send(b"H")
        self.through(b"3")


def snapshot_count(server):
    output = server.control(
        '(format t "~%PIN-COUNT=~d~%" '
        '(sb-thread:with-mutex (pg::*snapshot-count-lock*) pg::*active-snapshots*))')
    return int(output.split("PIN-COUNT=")[-1].splitlines()[0])


def check_dml_bind(reader, writer):
    writer.query("CREATE TABLE portal_dml_target(id int)")
    writer.query("CREATE TABLE portal_dml_source(id int)")
    writer.query("INSERT INTO portal_dml_source VALUES (5)")
    writer.query("SET statement_timeout = '500ms'")
    for statement, sql, tag, blocked in (
            ("insert_rows", "INSERT INTO portal_dml_target SELECT id FROM portal_dml_source",
             "INSERT 0 2", ("portal_dml_target", "portal_dml_source")),
            ("update_rows", "UPDATE portal_dml_target SET id = id + 10",
             "UPDATE 2", ("portal_dml_target",)),
            ("delete_rows", "DELETE FROM portal_dml_target",
             "DELETE 2", ("portal_dml_target",))):
        reader.send(b"P", statement.encode() + b"\0" + sql.encode() + b"\0"
                    + struct.pack("!H", 0))
        # Sync releases Parse's transaction. Bind must establish its own protection.
        reader.send(b"S")
        reader.through(b"1")
        reader.through(b"Z")
        reader.bind(statement, "bound_dml")
        reader.send(b"H")
        reader.through(b"2")
        for table in blocked:
            assert writer.observe(f"DROP TABLE {table}") == ("error", "57014"), table
        if statement == "insert_rows":
            assert writer.query("SELECT id FROM portal_dml_target") == []
            writer.query("INSERT INTO portal_dml_source VALUES (6)")
        assert reader.execute("bound_dml") == ([], [tag], b"I", []), statement
    writer.query("DROP TABLE portal_dml_target, portal_dml_source")
    writer.query("RESET statement_timeout")


def check_repeatable_read_dml_bind(server, reader, writer):
    writer.query("CREATE TABLE bind_rr_target(id int)")
    writer.query("CREATE TABLE bind_rr_source(id int)")
    writer.query("INSERT INTO bind_rr_source VALUES (1)")
    reader.send(b"P", b"rr_insert\0INSERT INTO bind_rr_target "
                b"SELECT id FROM bind_rr_source\0" + struct.pack("!H", 0))
    reader.send(b"S")
    reader.through(b"1")
    assert reader.through(b"Z") == b"I"

    reader.query("SET statement_timeout='2s'")
    reader.query("BEGIN ISOLATION LEVEL REPEATABLE READ")
    reader.bind("rr_insert", "rr_dml")
    reader.send(b"H")
    reader.through(b"2")
    writer.query("INSERT INTO bind_rr_source VALUES (2)")
    assert writer.query("SELECT id FROM bind_rr_target") == []

    marker = server.root / "truncate-waiting-for-bind-schema-lease"
    server.control("(defvar pg::*bind-test-wait* "
                   "(symbol-function 'pg::condition-wait-with-timeout))")
    try:
        server.control(
            "(setf (symbol-function 'pg::condition-wait-with-timeout) "
            "(lambda (queue mutex) (when (eq queue pg::*schema-lock-cv*) "
            f'(with-open-file (marker "{marker}" :direction :output '
            ':if-exists :supersede) (write-line "waiting" marker))) '
            "(funcall pg::*bind-test-wait* queue mutex)))")
        writer.send(b"Q", b"TRUNCATE bind_rr_target\0")
        wire.wait_for_file(marker, server.process)
        assert reader.execute("rr_dml") == ([], ["INSERT 0 1"], b"T", [])
        assert reader.query("SELECT id FROM bind_rr_target") == [["1"]]
        reader.query("COMMIT")
        assert writer.through(b"C") == b"TRUNCATE TABLE\0"
        assert writer.through(b"Z") == b"I"
    finally:
        server.control("(setf (symbol-function 'pg::condition-wait-with-timeout) "
                       "pg::*bind-test-wait*)")
    reader.query("RESET statement_timeout")
    assert writer.query("SELECT id FROM bind_rr_target") == []

    reader.query("BEGIN ISOLATION LEVEL READ COMMITTED")
    reader.bind("rr_insert", "rc_dml")
    reader.send(b"H")
    reader.through(b"2")
    assert reader.observe(
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
    ) == ("error", "25001")
    reader.query("ROLLBACK")


def check_repeatable_read_select_bind(server, reader, writer):
    writer.query("CREATE TABLE bind_rr_select(id int)")
    writer.query("INSERT INTO bind_rr_select VALUES (1)")
    reader.prepare("bind_rr_select", bind=False, statement="rr_select")
    reader.send(b"S")
    assert reader.through(b"Z") == b"I"
    reader.query("BEGIN ISOLATION LEVEL REPEATABLE READ")
    server.control("(defvar pg::*bind-test-result-columns* "
                   "(symbol-function 'pg::extended-select-result-columns))")
    try:
        server.control(
            "(setf (symbol-function 'pg::extended-select-result-columns) "
            "(lambda (ast &optional register-p) "
            "(when register-p "
            "(assert (pg::backend-transaction-snapshot pg::*backend*)) "
            "(assert (pg::backend-holds-schema-read-lock-p))) "
            "(funcall pg::*bind-test-result-columns* ast register-p)))")
        reader.bind("rr_select", "rr_select_portal")
        reader.send(b"H")
        reader.through(b"2")
    finally:
        server.control("(setf (symbol-function 'pg::extended-select-result-columns) "
                       "pg::*bind-test-result-columns*)")
    writer.query("INSERT INTO bind_rr_select VALUES (2)")
    assert reader.execute("rr_select_portal") == ([["1"]], ["SELECT 1"], b"T", [])
    reader.query("COMMIT")
    writer.query("TRUNCATE bind_rr_select")
    assert writer.query("SELECT id FROM bind_rr_select") == []


def check_snapshot_read_only_transition(reader, writer):
    writer.query("CREATE TABLE export_transition(id int)")
    writer.query("INSERT INTO export_transition VALUES (1)")
    reader.query("BEGIN ISOLATION LEVEL REPEATABLE READ")
    assert reader.query("SELECT id FROM export_transition") == [["1"]]
    writer.query("INSERT INTO export_transition VALUES (2)")
    writer.query("SET statement_timeout = '500ms'")
    assert writer.observe("CREATE TABLE export_schema_guard(id int)") == ("error", "57014")
    writer.query("RESET statement_timeout")
    reader.query("SET TRANSACTION READ ONLY")
    ((token,),) = reader.query("SELECT pg_export_snapshot()")
    writer.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
    writer.query(f"SET TRANSACTION SNAPSHOT '{token}'")
    reader.query("ROLLBACK")
    assert writer.query("SELECT id FROM export_transition") == [["1"]]
    writer.query("ROLLBACK")
    # TRUNCATE still works in a writable RR transaction with a snapshot.
    reader.query("BEGIN ISOLATION LEVEL REPEATABLE READ")
    reader.query("SELECT 1")
    reader.query("TRUNCATE export_transition")
    reader.query("SET TRANSACTION READ ONLY")
    assert reader.observe("SELECT pg_export_snapshot()") == ("error", "25001")
    reader.query("ROLLBACK")
    assert writer.query("SELECT id FROM export_transition ORDER BY id") == [["1"], ["2"]]
    writer.query("DROP TABLE export_transition")


def check_sorted_volatile_slot_projection(connection):
    rows = connection.query(
        "SELECT pg_create_logical_replication_slot('sort_slot','pgoutput') "
        "FROM (SELECT 2 AS k UNION ALL SELECT 1) AS source ORDER BY k LIMIT 1"
    )
    assert len(rows) == 1, rows
    assert connection.query(
        "SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'sort_slot'"
    ) == [["sort_slot"]]
    connection.query("SELECT pg_drop_replication_slot('sort_slot')")

    connection.query("CREATE TABLE slot_sort_names(k int, label text)")
    connection.query("INSERT INTO slot_sort_names VALUES (2,'slot_discarded'),(1,'slot_retained')")
    sql = "SELECT pg_create_logical_replication_slot(label,$1) FROM slot_sort_names ORDER BY k LIMIT 1"
    connection.send(b"P", b"slotstmt\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 25))
    connection.send(
        b"B", b"slotportal\0slotstmt\0" + struct.pack("!HHI", 0, 1, 8)
        + b"pgoutput" + struct.pack("!H", 0)
    )
    connection.send(b"H")
    connection.through(b"2")
    rows, commands, status, errors = connection.execute("slotportal")
    assert len(rows) == 1 and not errors, (rows, commands, status, errors)
    assert connection.query(
        "SELECT slot_name FROM pg_replication_slots "
        "WHERE slot_name IN ('slot_discarded','slot_retained')"
    ) == [["slot_retained"]]
    connection.query("SELECT pg_drop_replication_slot('slot_retained')")


def check_sorted_file_projection(connection):
    connection.query("CREATE TABLE projection_order(a int)")
    connection.query("INSERT INTO projection_order VALUES (0), (2)")
    assert connection.query(
        "SELECT pg_read_binary_file('PG_VERSION', 0, a - 1, false) "
        "FROM projection_order ORDER BY a DESC LIMIT 1"
    ) == [[r"\x31"]]
    assert connection.observe(
        "SELECT pg_read_binary_file('PG_VERSION', 0, -1, false)"
    ) == ("error", "22023")


def check_ddl_skip_validation(connection):
    connection.query("CREATE TABLE index_collision(a int, j json)")
    connection.query("CREATE INDEX existing_index ON index_collision(a)")
    for name in ("index_collision", "existing_index"):
        assert connection.observe(
            f"CREATE INDEX IF NOT EXISTS {name} ON index_collision(missing)"
        ) == ("error", "42703")
        assert connection.observe(
            f"CREATE INDEX IF NOT EXISTS {name} ON index_collision(j)"
        ) == ("error", "42704")
        for statement in (
            f"CREATE INDEX IF NOT EXISTS {name} ON index_collision(a)",
            f"CREATE SEQUENCE IF NOT EXISTS {name}",
        ):
            connection.send(b"Q", statement.encode() + b"\0")
            notices, commands = [], []
            while True:
                kind, body = connection.receive()
                assert kind != b"E", connection.error_fields(body)
                if kind == b"N":
                    notices.append(connection.error_fields(body)["C"])
                elif kind == b"C":
                    commands.append(body)
                elif kind == b"Z":
                    break
            assert notices == ["42P07"], (statement, notices)
            assert commands == [(" ".join(statement.split()[:2]) + "\0").encode()]


def check_index_column_limit(connection):
    keys = ", ".join(["a"] * 32)
    connection.query(f"CREATE INDEX max_keys ON index_collision({keys})")
    for target in ("too_many_keys", "IF NOT EXISTS max_keys", "IF NOT EXISTS index_collision"):
        assert connection.observe(
            f"CREATE INDEX {target} ON index_collision({keys}, a)"
        ) == ("error", "54011")
        assert connection.observe(
            f"CREATE INDEX {target} ON missing_index_target({keys}, a)"
        ) == ("error", "42P01")

    columns = ", ".join(f"k{i} int" for i in range(33))
    keys = ", ".join(f"k{i}" for i in range(32))
    connection.query(f"CREATE TABLE max_constraint({columns}, UNIQUE ({keys}))")
    connection.query(f"CREATE TABLE constraint_target({columns})")
    for constraint in ("UNIQUE", "PRIMARY KEY"):
        assert connection.observe(
            f"CREATE TABLE oversize_constraint({columns}, {constraint} ({keys}, k32))"
        ) == ("error", "54011")
        assert connection.observe(
            f"ALTER TABLE constraint_target ADD {constraint} ({keys}, k32)"
        ) == ("error", "54011")


def check_repeated_inline_unique(connection):
    for table, clauses in (
            ("unnamed_inline", "UNIQUE NULLS NOT DISTINCT UNIQUE"),
            ("named_inline", "CONSTRAINT first_unique UNIQUE NULLS NOT DISTINCT "
             "CONSTRAINT second_unique UNIQUE")):
        connection.query(f"CREATE TABLE {table}(a int {clauses})")
        connection.query(f"INSERT INTO {table} VALUES (NULL)")
        assert connection.observe(
            f"INSERT INTO {table} VALUES (NULL)"
        ) == ("error", "23505")
    for name in ("first_unique", "second_unique"):
        assert connection.observe(
            f"CREATE INDEX {name} ON named_inline(a)"
        ) == ("error", "42P07")


def check_equivalent_inline_keys(connection):
    for table, clauses, surviving in (
            ("pk_unique", "PRIMARY KEY UNIQUE", "pk_unique_pkey"),
            ("mixed_pk", "PRIMARY KEY, UNIQUE (a)", "mixed_pk_pkey"),
            ("mixed_unique", "UNIQUE, PRIMARY KEY (a)", "mixed_unique_pkey"),
            ("mixed_repeated", "CONSTRAINT mixed_key UNIQUE, CONSTRAINT mixed_key UNIQUE (a)",
             "mixed_key"),
            ("mixed_transferred", "PRIMARY KEY, CONSTRAINT mixed_named UNIQUE (a)",
             "mixed_named"),
            ("named_pk", "PRIMARY KEY CONSTRAINT transferred UNIQUE", "transferred"),
            ("preferred_pk", "CONSTRAINT redundant UNIQUE CONSTRAINT preferred PRIMARY KEY",
             "preferred"),
            ("named_first", "CONSTRAINT named_first_key UNIQUE UNIQUE", "named_first_key"),
            ("named_last", "UNIQUE CONSTRAINT named_last_key UNIQUE", "named_last_key"),
            ("repeated_name", "CONSTRAINT same_key UNIQUE CONSTRAINT same_key UNIQUE",
             "same_key"),
            ("different_names", "CONSTRAINT retained_key UNIQUE CONSTRAINT redundant_key UNIQUE",
             "retained_key")):
        connection.query(f"CREATE TABLE {table}(a int {clauses})")
        assert connection.observe(f"INSERT INTO {table} VALUES (1), (1)") == ("error", "23505")
        connection.query(f"ALTER TABLE {table} DROP CONSTRAINT {surviving}")
        connection.query(f"INSERT INTO {table} VALUES (1), (1)")
        assert connection.query(f"SELECT count(*) FROM {table}") == [["2"]]

    connection.query("CREATE TABLE table_first(CONSTRAINT declared_first UNIQUE (a), "
                     "a int CONSTRAINT declared_second UNIQUE)")
    connection.query("ALTER TABLE table_first DROP CONSTRAINT declared_first")
    connection.query("INSERT INTO table_first VALUES (1), (1)")
    assert connection.query("SELECT count(*) FROM table_first") == [["2"]]

    assert connection.observe(
        "CREATE TABLE mixed_two_primary(a int PRIMARY KEY, PRIMARY KEY (a))"
    ) == ("error", "42P16")
    connection.query("CREATE TABLE mixed_distinct(a int UNIQUE, "
                     "CONSTRAINT mixed_not_distinct UNIQUE NULLS NOT DISTINCT (a))")
    connection.query("ALTER TABLE mixed_distinct DROP CONSTRAINT mixed_distinct_a_key")
    connection.query("INSERT INTO mixed_distinct VALUES (NULL)")
    assert connection.observe(
        "INSERT INTO mixed_distinct VALUES (NULL)"
    ) == ("error", "23505")

    connection.query("CREATE TABLE pk_distinct(a int PRIMARY KEY "
                     "CONSTRAINT pk_distinct_unique UNIQUE NULLS NOT DISTINCT)")
    connection.query("ALTER TABLE pk_distinct DROP CONSTRAINT pk_distinct_pkey")
    assert connection.observe("INSERT INTO pk_distinct VALUES (1), (1)") == ("error", "23505")


def run():
    server = wire.OwnedLisp()
    writer = reader = None
    try:
        server.start()
        writer, reader = Connection(server.port), Connection(server.port)
        check_dml_bind(reader, writer)
        check_repeatable_read_dml_bind(server, reader, writer)
        check_repeatable_read_select_bind(server, reader, writer)
        check_snapshot_read_only_transition(reader, writer)
        check_sorted_volatile_slot_projection(writer)
        check_sorted_file_projection(writer)
        check_ddl_skip_validation(writer)
        check_index_column_limit(writer)
        check_repeated_inline_unique(writer)
        check_equivalent_inline_keys(writer)
        writer.query("CREATE TABLE portal_rows(id int)")
        for isolation, bind_first, explicit, expected in (
                ("READ COMMITTED", True, True, []),
                ("REPEATABLE READ", False, True, []),
                ("READ COMMITTED", True, False, []),
                ("READ COMMITTED", False, True, [["1"]])):
            writer.query("DELETE FROM portal_rows")
            if explicit:
                reader.query(f"BEGIN ISOLATION LEVEL {isolation}")
            reader.prepare("portal_rows", bind=bind_first)
            writer.query("INSERT INTO portal_rows VALUES(1)")
            if not bind_first:
                reader.bind()
            actual = reader.execute()
            assert actual == (expected, [f"SELECT {len(expected)}"],
                              b"T" if explicit else b"I", []), (
                                  isolation, bind_first, explicit, actual)
            reader.query("ROLLBACK")
            # Named prepared statements survive transaction end.
            reader.send(b"C", b"Sstmt\0")
            reader.send(b"S")
            reader.through(b"Z")

        # A bound RC portal must pin a deleted version through VACUUM.
        writer.query("DELETE FROM portal_rows")
        writer.query("INSERT INTO portal_rows VALUES(7)")
        baseline = snapshot_count(server)
        reader.query("BEGIN")
        reader.prepare("portal_rows")
        assert snapshot_count(server) == baseline + 1
        writer.query("DELETE FROM portal_rows")
        writer.query("VACUUM portal_rows")
        assert reader.execute() == ([["7"]], ["SELECT 1"], b"T", [])
        reader.close_portal()
        assert snapshot_count(server) == baseline
        reader.query("ROLLBACK")

        # The saved command ID also excludes this transaction's later writes.
        reader.query("BEGIN")
        reader.bind(statement="stmt", portal="own_write")
        reader.send(b"H")
        reader.through(b"2")
        reader.query("INSERT INTO portal_rows VALUES(8)")
        assert reader.execute("own_write") == ([], ["SELECT 0"], b"T", [])
        reader.query("ROLLBACK")

        # Replacement, errors, commit and disconnect all retire portal pins.
        reader.query("BEGIN")
        reader.prepare("portal_rows", statement="", portal="")
        assert snapshot_count(server) == baseline + 1
        reader.bind(statement="", portal="")
        reader.send(b"H")
        reader.through(b"2")
        assert snapshot_count(server) == baseline + 1
        failed = reader.execute("missing")
        assert failed[3] and failed[3][0]["C"] == "34000", failed
        assert snapshot_count(server) == baseline
        reader.query("ROLLBACK")
        for ending in ("COMMIT", "ROLLBACK"):
            reader.query("BEGIN")
            reader.bind(statement="stmt", portal="")
            reader.send(b"H")
            reader.through(b"2")
            assert snapshot_count(server) == baseline + 1
            reader.query(ending)
            assert snapshot_count(server) == baseline
        reader.bind(statement="stmt", portal="")
        reader.send(b"H")
        reader.through(b"2")
        assert snapshot_count(server) == baseline + 1
        reader.close()
        reader = None
        deadline = time.monotonic() + 5
        while snapshot_count(server) != baseline:
            assert time.monotonic() < deadline, "disconnected portal retained snapshot"
            time.sleep(0.02)

        # Even a constant SELECT portal pins a snapshot without a relation/XID.
        # Its owner must drain that pin rather than coordinate XID wraparound.
        reader = Connection(server.port)
        reader.send(b"P", b"constant\0SELECT 1\0" + struct.pack("!H", 0))
        reader.bind(statement="constant", portal="wrap_pin")
        reader.send(b"H")
        reader.through(b"2")
        server.control(
            "(defparameter pg::*portal-test-owner* nil) "
            "(sb-thread:with-mutex (pg::*extended-query-state-registry-lock*) "
            "(maphash (lambda (backend state) "
            "(when (gethash \"wrap_pin\" (pg::extended-query-state-portals state)) "
            "(setf pg::*portal-test-owner* backend))) pg::*extended-query-state-registry*)) "
            "(assert pg::*portal-test-owner*) "
            "(assert (null (pg::backend-xid pg::*portal-test-owner*))) "
            "(assert (pg::extended-backend-holds-snapshot-p pg::*portal-test-owner*)) "
            "(assert (not (pg::backend-xid-wrap-resource-free-p pg::*portal-test-owner*)))")
        try:
            reader.close_portal("wrap_pin")
            server.control(
                "(assert (not (pg::extended-backend-holds-snapshot-p pg::*portal-test-owner*))) "
                "(assert (pg::backend-xid-wrap-resource-free-p pg::*portal-test-owner*))")
        finally:
            server.control("(makunbound 'pg::*portal-test-owner*)")
        reader.send(b"S")
        reader.through(b"Z")
        assert snapshot_count(server) == baseline
        print("portal-snapshot: PASS (Parse/Bind timing, VACUUM retention and lifecycle cleanup)")
    finally:
        if reader is not None:
            reader.close()
        if writer is not None:
            writer.close()
        server.stop()
        print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()
