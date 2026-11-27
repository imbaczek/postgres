"""Exercise the outside-lock row preparation needed by foreign-key checks."""

import contextlib
import runpy
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().with_name("interop")))
from binary_values import bind, frame, parse
from parking_wire import WireError

fixture = runpy.run_path(str(Path(__file__).with_name("command-complete.py")))
OwnedLisp, Connection = fixture["OwnedLisp"], fixture["Connection"]


def wait_for_relation_waiter(server, pending):
    deadline = time.monotonic() + 5
    while not pending.done() and time.monotonic() < deadline:
        if "\nT\n" in server.control(
            "(sb-thread:with-mutex (*relation-lock-mutex*) "
            "(plusp (hash-table-count *relation-wait-dependencies*)))"
        ):
            return
    raise AssertionError("statement did not wait for the other transaction")


def verify_existing_rows_validation(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE validation_parent (id integer PRIMARY KEY)")
        connection.query("CREATE TABLE validation_child (parent_id bigint)")
        connection.query("INSERT INTO validation_child VALUES (7)")
        probe = """
            (let* ((*backend* (make-backend :database "postgres"))
                   (parent (lookup-table "validation_parent"))
                   (child (lookup-table "validation_child"))
                   (key (cook-foreign-key child parent '(:columns ("parent_id")) 1))
                   (index (find-index-by-oid (pg-foreign-key-referenced-index-oid key))))
              (handler-case
                  (progn (validate-existing-foreign-key child parent key index) :valid)
                (pg-error (condition) (pg-error-sqlstate condition))))
        """
        assert "23503" in server.control(probe)
        add = "ALTER TABLE validation_child ADD FOREIGN KEY (parent_id) REFERENCES validation_parent"
        try:
            connection.query(add)
        except WireError as error:
            assert error.sqlstate == "23503", error
        else:
            raise AssertionError("ALTER TABLE validated an existing orphan")
        connection.query("INSERT INTO validation_parent VALUES (7)")
        assert ":VALID" in server.control(probe)
        connection.query("BEGIN")
        connection.query(add)
        connection.query("ROLLBACK")
        connection.query("INSERT INTO validation_child VALUES (8)")
        connection.query(
            "ALTER TABLE validation_child ADD CONSTRAINT needs_validation "
            "FOREIGN KEY (parent_id) REFERENCES validation_parent NOT VALID"
        )
        validate = "ALTER TABLE validation_child VALIDATE CONSTRAINT needs_validation"
        connection.query("UPDATE validation_child SET parent_id=parent_id WHERE parent_id=8")
        for statement in ("INSERT INTO validation_child VALUES (9)", validate):
            try:
                connection.query(statement)
            except WireError as error:
                assert error.sqlstate == "23503", error
            else:
                raise AssertionError(f"NOT VALID lost foreign-key enforcement: {statement}")
        with contextlib.closing(Connection(server.port)) as writer, ThreadPoolExecutor(max_workers=1) as worker:
            writer.query("BEGIN")
            writer.query("INSERT INTO validation_parent VALUES (8)")
            pending = worker.submit(connection.query, validate)
            try:
                wait_for_relation_waiter(server, pending)
            finally:
                writer.query("ROLLBACK")
            try:
                pending.result(timeout=10)
            except WireError as error:
                assert error.sqlstate == "23503", error
            else:
                raise AssertionError("validation accepted a parent that rolled back")
        connection.query("INSERT INTO validation_parent VALUES (8)")
        state = "SELECT convalidated FROM pg_constraint WHERE conname='needs_validation'"
        connection.query("BEGIN")
        connection.query(validate)
        assert connection.query(state) == [["t"]]
        connection.query("ROLLBACK")
        assert connection.query(state) == [["f"]]
        connection.query(validate)
        assert connection.query(state) == [["t"]]


def verify_parent_changes(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE fk_parent (id integer PRIMARY KEY)")
        connection.query("CREATE TABLE fk_child (parent_id bigint)")
        connection.query("INSERT INTO fk_parent VALUES (1), (2)")
        connection.query("INSERT INTO fk_child VALUES (1)")
        connection.query("ALTER TABLE fk_child ADD CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES fk_parent")
        connection.query("BEGIN")
        try:
            connection.query("DROP TABLE fk_parent")
        except WireError as error:
            assert error.sqlstate == "2BP01", error
        else:
            raise AssertionError("DROP TABLE staged a change that invalidated a surviving foreign key")
        connection.query("ROLLBACK")
        connection.query("BEGIN")
        try:
            connection.query("ALTER TABLE fk_parent DROP COLUMN id CASCADE")
        except WireError as error:
            assert error.sqlstate == "2BP01", error
        else:
            raise AssertionError("DROP COLUMN staged the removal of a referenced key")
        connection.query("ROLLBACK")
        connection.query("BEGIN")
        connection.query("ALTER TABLE fk_child DROP COLUMN parent_id")
        assert connection.query("SELECT oid FROM pg_constraint WHERE conname='fk_child_parent'") == []
        connection.query("ROLLBACK")
        assert len(connection.query("SELECT oid FROM pg_constraint WHERE conname='fk_child_parent'")) == 1
        for statement, sqlstate in (
            ("DROP TABLE fk_parent", "2BP01"),
            ("ALTER TABLE fk_parent DROP COLUMN id", "2BP01"),
            ("TRUNCATE fk_parent", "0A000"),
        ):
            try:
                connection.query(statement)
            except WireError as error:
                assert error.sqlstate == sqlstate, error
            else:
                raise AssertionError(f"DDL removed a live foreign-key dependency: {statement}")
        for statement in (
            "DELETE FROM fk_parent WHERE id=1",
            "UPDATE fk_parent SET id=3 WHERE id=1",
            "INSERT INTO fk_parent VALUES (1) ON CONFLICT (id) DO UPDATE SET id=3",
        ):
            try:
                connection.query(statement)
            except WireError as error:
                assert error.sqlstate == "23503", error
            else:
                raise AssertionError(f"parent change orphaned a referencing row: {statement}")
        connection.query("UPDATE fk_parent SET id=3 WHERE id=2")
        assert connection.query("SELECT id FROM fk_parent ORDER BY id") == [["1"], ["3"]]
        for statement in ("INSERT INTO fk_child VALUES (99)", "UPDATE fk_child SET parent_id=99"):
            try:
                connection.query(statement)
            except WireError as error:
                assert error.sqlstate == "23503", error
            else:
                raise AssertionError(f"child change accepted a missing parent: {statement}")


def verify_empty_cascade_does_not_admit_grandchild(server):
    with contextlib.closing(Connection(server.port)) as parent, contextlib.closing(Connection(server.port)) as ddl:
        parent.query("CREATE TABLE empty_cascade_parent (id integer PRIMARY KEY)")
        parent.query(
            "CREATE TABLE empty_cascade_child (id integer PRIMARY KEY "
            "REFERENCES empty_cascade_parent ON DELETE CASCADE)"
        )
        parent.query(
            "CREATE TABLE empty_cascade_leaf (id integer "
            "REFERENCES empty_cascade_child ON DELETE CASCADE)"
        )
        parent.query("INSERT INTO empty_cascade_parent VALUES (1)")
        parent.query("BEGIN")
        parent.query("DELETE FROM empty_cascade_parent WHERE id=1")
        ddl.query("SET statement_timeout = '2s'")
        ddl.query("DROP TABLE empty_cascade_leaf")
        parent.query("COMMIT")


def verify_batched_insert_savepoint(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE batch_parent (id integer PRIMARY KEY)")
        connection.query("CREATE TABLE batch_child (id integer NOT NULL REFERENCES batch_parent)")
        rows = ",".join(f"({value})" for value in range(130))
        connection.query(f"INSERT INTO batch_parent VALUES {rows}")
        connection.query("BEGIN")
        connection.query("INSERT INTO batch_child VALUES (0)")
        connection.query("SAVEPOINT batch_rows")
        connection.query(f"INSERT INTO batch_child VALUES {rows}")
        assert connection.query("SELECT count(*) FROM batch_child") == [["131"]]
        connection.query("ROLLBACK TO batch_rows")
        assert connection.query("SELECT id FROM batch_child") == [["0"]]
        connection.query("COMMIT")
        try:
            connection.query(f"INSERT INTO batch_child VALUES {rows},(999)")
        except WireError as error:
            assert error.sqlstate == "23503", error
        else:
            raise AssertionError("batched insert committed a missing foreign key")
        assert connection.query("SELECT id FROM batch_child") == [["0"]]


def verify_batched_cascade_savepoint(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE cascade_batch_parent (id integer PRIMARY KEY)")
        connection.query(
            "CREATE TABLE cascade_batch_child (seq integer, parent_id integer "
            "REFERENCES cascade_batch_parent ON DELETE CASCADE)"
        )
        connection.query("INSERT INTO cascade_batch_parent VALUES (1),(2)")
        rows = ",".join(f"({value},1)" for value in range(250))
        connection.query(f"INSERT INTO cascade_batch_child VALUES {rows},(250,2)")
        # Leave a version with aborted xmax to exercise the scalar fallback.
        connection.query("BEGIN")
        connection.query("UPDATE cascade_batch_child SET parent_id=2 WHERE seq=70")
        connection.query("ROLLBACK")
        connection.query("BEGIN")
        connection.query("INSERT INTO cascade_batch_child VALUES (251,1)")
        connection.query("SAVEPOINT before_cascade")
        connection.query("DELETE FROM cascade_batch_parent WHERE id=1")
        assert connection.query("SELECT seq FROM cascade_batch_child") == [["250"]]
        connection.query("ROLLBACK TO before_cascade")
        assert connection.query("SELECT count(*) FROM cascade_batch_child") == [["252"]]
        assert connection.query("SELECT parent_id FROM cascade_batch_child WHERE seq=251") == [["1"]]
        connection.query("DELETE FROM cascade_batch_parent WHERE id=1")
        connection.query("COMMIT")
        assert connection.query("SELECT seq FROM cascade_batch_child") == [["250"]]


def verify_batched_cascade_rechecks_updated_row(server):
    with contextlib.closing(Connection(server.port)) as deleter, contextlib.closing(
        Connection(server.port)
    ) as updater, ThreadPoolExecutor(max_workers=1) as worker:
        deleter.query("INSERT INTO cascade_batch_parent VALUES (1)")
        deleter.query("INSERT INTO cascade_batch_child VALUES (1,1),(2,1),(3,1)")
        deleter.query("SET application_name = 'test-cascade-batch'")
        server.control("""
            (defparameter *test-cascade-batch-original* (fdefinition 'heapam-delete-page-candidates))
            (defparameter *test-cascade-batch-ready* (sb-thread:make-semaphore))
            (defparameter *test-cascade-batch-resume* (sb-thread:make-semaphore))
            (defparameter *test-cascade-batch-paused* nil)
            (setf (fdefinition 'heapam-delete-page-candidates)
                  (lambda (node block candidates xid cid)
                    (when (and (not *test-cascade-batch-paused*)
                               (equal (backend-application-name *backend*) "test-cascade-batch"))
                      (setf *test-cascade-batch-paused* t)
                      (sb-thread:signal-semaphore *test-cascade-batch-ready*)
                      (assert (sb-thread:wait-on-semaphore *test-cascade-batch-resume* :timeout 10)))
                    (funcall *test-cascade-batch-original* node block candidates xid cid)))
        """)
        deletion = worker.submit(deleter.query, "DELETE FROM cascade_batch_parent WHERE id=1")
        try:
            try:
                assert "\nT\n" in server.control(
                    "(not (null (sb-thread:wait-on-semaphore *test-cascade-batch-ready* :timeout 5)))"
                )
                updater.query("SET statement_timeout = '2s'")
                updater.query("UPDATE cascade_batch_child SET parent_id=2 WHERE seq=2")
            finally:
                server.control("(sb-thread:signal-semaphore *test-cascade-batch-resume*)")
            deletion.result(timeout=10)
        finally:
            server.control("(setf (fdefinition 'heapam-delete-page-candidates) *test-cascade-batch-original*)")
        assert updater.query("SELECT seq, parent_id FROM cascade_batch_child ORDER BY seq") == [
            ["2", "2"], ["250", "2"]
        ]


def verify_probe_skips_deleted_versions(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE probe_parent (id varchar(127) PRIMARY KEY)")
        connection.query("CREATE TABLE probe_child (id varchar(127) REFERENCES probe_parent)")
        key = "'" + "x" * 127 + "'"
        connection.query("BEGIN")
        for _ in range(75):
            connection.query(f"INSERT INTO probe_parent VALUES ({key})")
            connection.query(f"DELETE FROM probe_parent WHERE id={key}")
        connection.query(f"INSERT INTO probe_parent VALUES ({key})")
        connection.query("COMMIT")
        connection.query(f"INSERT INTO probe_child VALUES ({key})")
        assert connection.query("SELECT count(*) FROM probe_child") == [["1"]]


def verify_child_parent_races(server):
    with contextlib.closing(Connection(server.port)) as first, contextlib.closing(
        Connection(server.port)
    ) as second, ThreadPoolExecutor(max_workers=1) as worker:
        for offset, parent_change in enumerate((
            "DELETE FROM fk_parent WHERE id={key}",
            "UPDATE fk_parent SET id={replacement} WHERE id={key}",
        )):
            for child_first in (False, True):
                key = 10 + 2 * offset + int(child_first)
                first.query(f"INSERT INTO fk_parent VALUES ({key})")
                parent_statement = parent_change.format(key=key, replacement=100 + key)
                child_statement = f"INSERT INTO fk_child VALUES ({key})"
                before, after = (
                    (child_statement, parent_statement) if child_first else (parent_statement, child_statement)
                )
                first.query("BEGIN")
                first.query(before)
                pending = worker.submit(second.query, after)
                try:
                    wait_for_relation_waiter(server, pending)
                finally:
                    first.query("COMMIT")
                try:
                    pending.result(timeout=10)
                except WireError as error:
                    assert error.sqlstate == "23503", error
                else:
                    raise AssertionError(f"concurrent writers committed an orphan for key {key}")
                expected = [[str(key)]] if child_first else []
                assert first.query(f"SELECT id FROM fk_parent WHERE id={key}") == expected
                assert first.query(f"SELECT parent_id FROM fk_child WHERE parent_id={key}") == expected


def verify_composite_self_reference(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query(
            "CREATE TABLE fk_self (a integer, b integer, ref_a integer, ref_b integer, PRIMARY KEY (a,b), "
            "CONSTRAINT fk_self_reference FOREIGN KEY (ref_b,ref_a) REFERENCES fk_self (b,a) MATCH FULL)"
        )
        assert connection.query(
            "SELECT k.contype, k.conkey::text, k.confkey::text, k.confmatchtype, "
            "k.convalidated, i.indisunique FROM pg_constraint k "
            "JOIN pg_class c ON c.oid=k.conrelid JOIN pg_index i ON i.indexrelid=k.conindid "
            "WHERE c.relname='fk_self'"
        ) == [["f", "{4,3}", "{2,1}", "f", "t", "t"]]
        connection.query("INSERT INTO fk_self VALUES (1,2,3,4), (3,4,NULL,NULL)")
        try:
            connection.query("INSERT INTO fk_self VALUES (5,6,1,NULL)")
        except WireError as error:
            assert error.sqlstate == "23503", error
        else:
            raise AssertionError("MATCH FULL accepted a partially null key")
        assert connection.query("SELECT a,b FROM fk_self ORDER BY a") == [["1", "2"], ["3", "4"]]
        try:
            connection.query(
                "ALTER TABLE fk_self ADD CONSTRAINT fk_self_reference UNIQUE (ref_a)"
            )
        except WireError as error:
            assert error.sqlstate == "42710", error
        else:
            raise AssertionError("UNIQUE reused an existing foreign-key constraint name")


def verify_inline_create_reference(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("BEGIN")
        connection.query("CREATE TABLE new_fk_parent (id integer PRIMARY KEY)")
        connection.query("CREATE TABLE new_fk_child (parent_id bigint REFERENCES new_fk_parent)")
        connection.query("INSERT INTO new_fk_parent VALUES (1)")
        connection.query("INSERT INTO new_fk_child VALUES (1)")
        connection.query("ROLLBACK")
        assert connection.query("SELECT oid FROM pg_class WHERE relname='new_fk_child'") == []
        assert connection.query("SELECT oid FROM pg_constraint WHERE conname='new_fk_child_parent_id_fkey'") == []


def verify_referential_actions(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE cascade_parent (id integer PRIMARY KEY)")
        connection.query(
            "CREATE TABLE cascade_child (id integer PRIMARY KEY, FOREIGN KEY (id) "
            "REFERENCES cascade_parent ON UPDATE CASCADE ON DELETE CASCADE)"
        )
        connection.query(
            "CREATE TABLE cascade_leaf (id integer REFERENCES cascade_child ON UPDATE CASCADE ON DELETE CASCADE)"
        )
        connection.query(
            "CREATE TABLE null_child (id integer REFERENCES cascade_parent ON DELETE SET NULL)"
        )
        connection.query(
            "CREATE TABLE default_child (id integer DEFAULT 5 REFERENCES cascade_parent ON DELETE SET DEFAULT)"
        )
        connection.query("INSERT INTO cascade_parent VALUES (1), (5), (8), (9)")
        connection.query("INSERT INTO cascade_child VALUES (1)")
        connection.query("INSERT INTO cascade_leaf VALUES (1)")
        connection.query("INSERT INTO null_child VALUES (8)")
        connection.query("INSERT INTO default_child VALUES (8), (9)")
        connection.query("UPDATE cascade_parent SET id=2 WHERE id=1")
        assert connection.query("SELECT id FROM cascade_leaf") == [["2"]]
        connection.query("BEGIN")
        connection.query("DELETE FROM cascade_parent WHERE id IN (2,8)")
        assert connection.query("SELECT id FROM cascade_leaf") == []
        assert connection.query("SELECT id FROM null_child") == [[None]]
        assert connection.query("SELECT id FROM default_child ORDER BY id") == [["5"], ["9"]]
        connection.query("ROLLBACK")
        assert connection.query("SELECT id FROM cascade_leaf") == [["2"]]
        assert connection.query("SELECT id FROM null_child") == [["8"]]
        connection.query("DELETE FROM cascade_parent WHERE id=5")
        try:
            connection.query("DELETE FROM cascade_parent WHERE id=9")
        except WireError as error:
            assert error.sqlstate == "23503", error
        else:
            raise AssertionError("SET DEFAULT accepted a default with no parent")
        assert connection.query("SELECT id FROM default_child ORDER BY id") == [["8"], ["9"]]
        connection.query(
            "CREATE TABLE unchanged_default (id integer DEFAULT 8 "
            "REFERENCES cascade_parent ON DELETE SET DEFAULT)"
        )
        connection.query("INSERT INTO unchanged_default VALUES (8)")
        connection.query("INSERT INTO cascade_parent VALUES (5)")
        try:
            connection.query("DELETE FROM cascade_parent WHERE id=8")
        except WireError as error:
            assert error.sqlstate == "23503", error
        else:
            raise AssertionError("SET DEFAULT left the reference on its deleted parent")
        assert connection.query("SELECT id FROM cascade_parent WHERE id=8") == [["8"]]
        connection.query(
            "CREATE TABLE cascade_cycle (id integer PRIMARY KEY, peer integer "
            "REFERENCES cascade_cycle ON DELETE CASCADE)"
        )
        connection.query("INSERT INTO cascade_cycle VALUES (1,2), (2,1)")
        connection.query("DELETE FROM cascade_cycle WHERE id=1")
        assert connection.query("SELECT id FROM cascade_cycle") == []
        connection.query("CREATE TABLE shift_parent (id integer PRIMARY KEY)")
        connection.query(
            "CREATE TABLE shift_child (id integer REFERENCES shift_parent ON UPDATE CASCADE)"
        )
        connection.query("INSERT INTO shift_parent VALUES (2), (1)")
        connection.query("INSERT INTO shift_child VALUES (1), (2)")
        connection.query("UPDATE shift_parent SET id=id+1")
        assert connection.query("SELECT id FROM shift_child ORDER BY id") == [["2"], ["3"]]


def verify_copy_foreign_keys(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query(
            "CREATE TABLE copy_child (parent_id integer REFERENCES fk_parent, payload text DEFAULT 'omitted')"
        )

        def copy_input(command, fields, terminal=b"c", terminal_payload=b""):
            connection.socket.sendall(frame(b"Q", command.encode() + b"\0"))
            kind, payload = connection.receive()
            while kind == b"C":
                kind, payload = connection.receive()
            assert kind == b"G" and payload[0] == 0, (kind, payload)
            connection.socket.sendall(
                b"".join(frame(b"d", chunk) for chunk in fields) + frame(terminal, terminal_payload)
            )
            completion, error = None, None
            while True:
                kind, payload = connection.receive()
                if kind == b"C":
                    completion = payload
                elif kind == b"E":
                    error = WireError(connection.error_fields(payload))
                elif kind == b"Z":
                    if error:
                        raise error
                    return completion

        assert copy_input("COPY copy_child FROM STDIN", [b"1\tpart\\", b"tend\n\\N\t\\141\n"]) == b"COPY 2\0"
        assert copy_input("COPY copy_child (parent_id) FROM STDIN", [b"1\n"]) == b"COPY 1\0"
        for data, terminal, terminal_payload, state in (
            ([b"1\n999\n"], b"c", b"", "23503"),
            ([b"1\ninvalid\n", b"1\n"], b"c", b"", "22P02"),
            ([b"1\n"], b"f", b"user aborted\0", "57014"),
        ):
            try:
                copy_input("COPY copy_child (parent_id) FROM STDIN", data, terminal, terminal_payload)
            except WireError as error:
                assert error.sqlstate == state, error
            else:
                raise AssertionError("invalid COPY committed")
        assert connection.query("SELECT payload FROM copy_child ORDER BY payload") == [
            ["a"], ["omitted"], ["part\tend"]
        ]
        connection.query("CREATE TABLE copy_lines (id integer)")
        for chunks in ([b"1\r", b"2\r"], [b"1\r", b"\n2\r", b"\n"], [b"1\n2"]):
            assert copy_input("COPY copy_lines FROM STDIN", chunks) == b"COPY 2\0"
        try:
            copy_input("COPY copy_lines FROM STDIN", [b"3\r\n4\n"])
        except WireError as error:
            assert error.sqlstate == "22P04", error
        else:
            raise AssertionError("COPY accepted mixed line endings")
        assert connection.query("SELECT id FROM copy_lines ORDER BY id") == [["1"]] * 3 + [["2"]] * 3
        for value, state in ((b"1", None), (b"999", "23503"), (b"invalid", "22P02")):
            connection.socket.sendall(
                parse("COPY copy_child (parent_id) FROM STDIN") + bind()
                + frame(b"E", b"\0\0\0\0\0") + frame(b"H", b"")
            )
            assert [connection.receive()[0] for _ in range(3)] == [b"1", b"2", b"G"]
            connection.socket.sendall(frame(b"d", value + b"\n") + frame(b"c", b"") + frame(b"S", b""))
            kind, payload = connection.receive()
            assert kind == (b"C" if state is None else b"E"), (kind, payload)
            if state is None:
                assert payload == b"COPY 1\0", payload
            else:
                assert connection.error_fields(payload)["C"] == state, payload
            assert connection.receive() == (b"Z", b"I")
        assert connection.query("SELECT count(*) FROM copy_child") == [["4"]]
        connection.query("CREATE TABLE copy_short (body varchar(3))")
        try:
            copy_input("COPY copy_short FROM STDIN", [b"abc\nabcd\n"])
        except WireError as error:
            assert error.sqlstate == "22001", error
        else:
            raise AssertionError("COPY silently truncated a varchar value")
        assert connection.query("SELECT body FROM copy_short") == []
        try:
            copy_input("INSERT INTO copy_short VALUES ('abc'); COPY copy_child (parent_id) FROM STDIN",
                       [b"1\n999\n"])
        except WireError as error:
            assert error.sqlstate == "23503", error
        else:
            raise AssertionError("multi-statement COPY accepted a missing parent")
        assert connection.query("SELECT body FROM copy_short") == []


def verify_deferred_constraints(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE deferred_parent (id integer PRIMARY KEY)")
        connection.query(
            "CREATE TABLE deferred_child (id integer REFERENCES deferred_parent "
            "DEFERRABLE INITIALLY DEFERRED)"
        )
        connection.query("BEGIN")
        connection.query("INSERT INTO deferred_child VALUES (1)")
        connection.query("INSERT INTO deferred_parent VALUES (1)")
        connection.query("COMMIT")
        connection.query("BEGIN")
        connection.query("DELETE FROM deferred_parent WHERE id=1")
        connection.query("DELETE FROM deferred_child WHERE id=1")
        connection.query("INSERT INTO deferred_child VALUES (2)")
        connection.query("DELETE FROM deferred_child WHERE id=2")
        connection.query("COMMIT")
        connection.query("BEGIN")
        connection.query("INSERT INTO deferred_child VALUES (3)")
        try:
            connection.query("COMMIT")
        except WireError as error:
            assert error.sqlstate == "23503", error
        else:
            raise AssertionError("COMMIT accepted a deferred orphan")
        assert connection.query("SELECT id FROM deferred_child") == []
        connection.query(
            "CREATE TABLE mode_child (id integer CONSTRAINT mode_fk REFERENCES deferred_parent DEFERRABLE)"
        )
        connection.query("BEGIN")
        connection.query("SET CONSTRAINTS ALL DEFERRED")
        connection.query("INSERT INTO mode_child VALUES (4)")
        connection.query("SET CONSTRAINTS deferred_child_id_fkey IMMEDIATE")
        try:
            connection.query("SET CONSTRAINTS mode_fk IMMEDIATE")
        except WireError as error:
            assert error.sqlstate == "23503", error
        else:
            raise AssertionError("SET CONSTRAINTS IMMEDIATE accepted a pending orphan")
        connection.query("ROLLBACK")
        connection.query("BEGIN")
        connection.query("SET CONSTRAINTS mode_fk DEFERRED")
        connection.query("INSERT INTO mode_child VALUES (4)")
        connection.query("INSERT INTO deferred_parent VALUES (4)")
        connection.query("SET CONSTRAINTS ALL IMMEDIATE")
        connection.query("COMMIT")
        assert connection.query("SELECT id FROM mode_child") == [["4"]]
        connection.query(
            "CREATE TABLE deferred_full (a integer, b integer, FOREIGN KEY (a,b) REFERENCES fk_self "
            "MATCH FULL DEFERRABLE INITIALLY DEFERRED)"
        )
        connection.query("BEGIN")
        connection.query("INSERT INTO deferred_full VALUES (1,NULL)")
        connection.query("DELETE FROM deferred_full")
        connection.query("COMMIT")
        connection.query("BEGIN")
        connection.query("INSERT INTO deferred_full VALUES (1,NULL)")
        try:
            connection.query("COMMIT")
        except WireError as error:
            assert error.sqlstate == "23503", error
        else:
            raise AssertionError("deferred MATCH FULL accepted a surviving partially null reference")


def verify_deferred_row_versions(server):
    with contextlib.closing(Connection(server.port)) as connection, contextlib.closing(
        Connection(server.port)
    ) as writer:
        connection.query("CREATE TABLE version_child (id integer, tag integer)")
        connection.query("INSERT INTO version_child VALUES (888,0)")
        connection.query(
            "ALTER TABLE version_child ADD FOREIGN KEY (id) REFERENCES deferred_parent "
            "DEFERRABLE INITIALLY DEFERRED NOT VALID"
        )
        connection.query("BEGIN")
        connection.query("INSERT INTO version_child VALUES (888,1)")
        connection.query("DELETE FROM version_child WHERE tag=1")
        connection.query("COMMIT")
        connection.query("BEGIN")
        connection.query("INSERT INTO version_child VALUES (888,1)")
        connection.query("UPDATE version_child SET tag=2 WHERE tag=1")
        try:
            connection.query("COMMIT")
        except WireError as error:
            assert error.sqlstate == "23503", error
        else:
            raise AssertionError("replacing a queued tuple lost its deferred check")
        assert connection.query("SELECT tag FROM version_child") == [["0"]]

        connection.query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        connection.query("SELECT count(*) FROM deferred_parent")
        writer.query("INSERT INTO deferred_parent VALUES (777)")
        connection.query("INSERT INTO version_child VALUES (777,1)")
        try:
            connection.query("COMMIT")
        except WireError as error:
            assert error.sqlstate == "23503", error
        else:
            raise AssertionError("deferred child probe used a parent outside the transaction snapshot")


def verify_partman_deferred_cascade(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE part_config (parent_table text PRIMARY KEY)")
        connection.query(
            "CREATE TABLE part_config_sub (sub_parent text PRIMARY KEY REFERENCES part_config "
            "ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)"
        )
        connection.query("BEGIN")
        connection.query("INSERT INTO part_config_sub VALUES ('parent_old')")
        connection.query("INSERT INTO part_config VALUES ('parent_old')")
        connection.query("COMMIT")
        connection.query("BEGIN")
        connection.query("UPDATE part_config SET parent_table='parent_new'")
        assert connection.query("SELECT sub_parent FROM part_config_sub") == [["parent_new"]]
        connection.query("DELETE FROM part_config")
        assert connection.query("SELECT sub_parent FROM part_config_sub") == []
        connection.query("ROLLBACK")
        assert connection.query("SELECT sub_parent FROM part_config_sub") == [["parent_old"]]


def verify_savepoint_heap_undo(server):
    with contextlib.closing(Connection(server.port)) as connection, contextlib.closing(
        Connection(server.port)
    ) as observer:
        connection.query("CREATE TABLE heap_undo (id integer PRIMARY KEY, payload text)")
        connection.query("ALTER TABLE heap_undo ALTER COLUMN payload SET STORAGE EXTERNAL")
        connection.query("INSERT INTO heap_undo VALUES (1,'" + "abcdefgh" * 8000 + "'), (2,'old')")
        connection.query("BEGIN")
        connection.query("SAVEPOINT before_writes")
        connection.query("UPDATE heap_undo SET id=3, payload='replaced' WHERE id=1")
        connection.query("DELETE FROM heap_undo WHERE id=2")
        connection.query("INSERT INTO heap_undo VALUES (4,'new')")
        connection.query("ROLLBACK TO SAVEPOINT before_writes")
        rows = "SELECT id, length(payload) FROM heap_undo ORDER BY id"
        restored = connection.query(rows)
        assert restored == [["1", "64000"], ["2", "3"]], restored
        connection.query("INSERT INTO heap_undo VALUES (4,'survives')")
        connection.query("RELEASE SAVEPOINT before_writes")
        connection.query("COMMIT")
        assert observer.query(rows) == [["1", "64000"], ["2", "3"], ["4", "8"]]


def verify_failed_savepoint_recovers(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("BEGIN")
        connection.query("INSERT INTO deferred_parent VALUES (51)")
        connection.query("SAVEPOINT changes")
        connection.query("INSERT INTO deferred_child VALUES (52)")
        try:
            connection.query("SET CONSTRAINTS ALL IMMEDIATE")
        except WireError as error:
            assert error.sqlstate == "23503", error
        else:
            raise AssertionError("pending orphan did not fail inside savepoint")
        connection.query("ROLLBACK TO changes")
        connection.query("INSERT INTO deferred_child VALUES (53)")
        connection.query("INSERT INTO deferred_parent VALUES (53)")
        connection.query("COMMIT")
        assert connection.query("SELECT id FROM deferred_child ORDER BY id") == [["53"]]
        assert connection.query("SELECT id FROM deferred_parent WHERE id>=51 AND id<=53 ORDER BY id") == [
            ["51"], ["53"]
        ]


def verify_savepoint_wakes_row_waiters(server):
    with contextlib.closing(Connection(server.port)) as first, contextlib.closing(
        Connection(server.port)
    ) as second, ThreadPoolExecutor(max_workers=1) as worker:
        first.query("CREATE TABLE savepoint_wait (id integer PRIMARY KEY, value integer)")
        for mutation, contender, expected in (
            ("INSERT INTO savepoint_wait VALUES (1,10)",
             "INSERT INTO savepoint_wait VALUES (1,20) RETURNING value", [["20"]]),
            ("UPDATE savepoint_wait SET value=30 WHERE id=1",
             "UPDATE savepoint_wait SET value=value+1 WHERE id=1 RETURNING value", [["21"]]),
            ("INSERT INTO savepoint_wait VALUES (1,30) ON CONFLICT (id) DO UPDATE SET value=30 WHERE false",
             "UPDATE savepoint_wait SET value=value+1 WHERE id=1 RETURNING value", [["22"]]),
        ):
            first.query("BEGIN")
            first.query("SAVEPOINT relinquish")
            first.query(mutation)
            pending = worker.submit(second.query, contender)
            try:
                wait_for_relation_waiter(server, pending)
                first.query("ROLLBACK TO relinquish")
                assert pending.result(timeout=5) == expected
            finally:
                first.query("ROLLBACK")


def verify_before_claim_rechecks_latest_version(server):
    with contextlib.closing(Connection(server.port)) as deleter, contextlib.closing(
        Connection(server.port)
    ) as updater:
        updater.query("CREATE TABLE claim_recheck (id integer PRIMARY KEY, value integer)")
        updater.query("INSERT INTO claim_recheck VALUES (1,10)")
        deleter.query("SET application_name = 'test-before-claim'")
        server.control("""
            (defparameter *test-claim-rows* nil)
            (defparameter *test-claim-ready* (sb-thread:make-semaphore))
            (defparameter *test-claim-resume* (sb-thread:make-semaphore))
            (setf (fdefinition 'heapam-delete-tid)
                  (let ((original (fdefinition 'heapam-delete-tid)))
                    (lambda (node columns block off xid cid
                             &optional (matches (constantly t)) on-delete outside-p candidate before-claim)
                      (funcall original node columns block off xid cid matches on-delete outside-p candidate
                               (if (equal (backend-application-name *backend*) "test-before-claim")
                                   (lambda (tuple)
                                     (assert (not (sb-thread:holding-mutex-p *write-lock*)))
                                     (push (aref (htup-values tuple) 1) *test-claim-rows*)
                                     (when (null (cdr *test-claim-rows*))
                                       (sb-thread:signal-semaphore *test-claim-ready*)
                                       (assert (sb-thread:wait-on-semaphore *test-claim-resume* :timeout 10))))
                                   before-claim)))))
        """)
        with ThreadPoolExecutor(max_workers=1) as worker:
            deletion = worker.submit(deleter.query, "DELETE FROM claim_recheck WHERE id=1 RETURNING value")
            try:
                assert "\nT\n" in server.control(
                    "(not (null (sb-thread:wait-on-semaphore *test-claim-ready* :timeout 5)))"
                )
                updater.query("UPDATE claim_recheck SET value=20 WHERE id=1")
            finally:
                server.control("(sb-thread:signal-semaphore *test-claim-resume*)")
            assert deletion.result(timeout=10) == [["20"]]
        assert "\n(20 10)\n" in server.control("*test-claim-rows*")


def verify_recovered_constraint(server):
    server.stop(immediate=True)
    server.start()
    with contextlib.closing(Connection(server.port)) as connection:
        assert connection.query("SELECT id, length(payload) FROM heap_undo ORDER BY id") == [
            ["1", "64000"], ["2", "3"], ["4", "8"]
        ]
        assert connection.query("SELECT id FROM deferred_child ORDER BY id") == [["53"]]
        assert connection.query("SELECT seq, parent_id FROM cascade_batch_child ORDER BY seq") == [
            ["2", "2"], ["250", "2"]
        ]
        assert connection.query(
            "SELECT convalidated FROM pg_constraint WHERE conname='needs_validation'"
        ) == [["t"]]
        try:
            connection.query("INSERT INTO validation_child VALUES (999)")
        except WireError as error:
            assert error.sqlstate == "23503", error
        else:
            raise AssertionError("recovery lost an enforced foreign key")


def verify_savepoint_logical_capture(server):
    with contextlib.closing(Connection(server.port)) as connection:
        connection.query("CREATE TABLE savepoint_capture (id integer PRIMARY KEY, body text)")
        connection.query("SELECT pg_create_logical_replication_slot('savepoint_capture_slot','pgoutput')")
        server.control("(setf *logical-capture-memory-bytes* 512)")
        connection.query("BEGIN")
        connection.query("INSERT INTO savepoint_capture VALUES (11,'keep')")
        connection.query("SAVEPOINT memory")
        connection.query("INSERT INTO savepoint_capture VALUES (12,'" + "x" * 1000 + "')")
        connection.query("ROLLBACK TO memory")
        connection.query("INSERT INTO savepoint_capture VALUES (13,'keep')")
        connection.query("COMMIT")
        connection.query("BEGIN")
        connection.query("INSERT INTO savepoint_capture VALUES (21,'" + "y" * 1000 + "')")
        connection.query("SAVEPOINT spilled")
        connection.query("INSERT INTO savepoint_capture VALUES (22,'discard')")
        connection.query("ROLLBACK TO spilled")
        connection.query("INSERT INTO savepoint_capture VALUES (23,'keep')")
        connection.query("COMMIT")
        result = server.control("""
            (call-with-logical-replication-slot "savepoint_capture_slot"
              (lambda (slot)
                (declare (ignore slot))
                (call-with-logical-reader-spools
                 (lambda ()
                   (let ((rows nil))
                     (dolist (transaction (wal-logical-slot-read-transactions "savepoint_capture_slot"))
                       (logical-transaction-map-changes
                        transaction
                        (lambda (change)
                          (when (eq :insert (logical-change-kind change))
                            (push (aref (logical-change-new-values change) 0) rows)))))
                     (format t "~&COPY-SAVEPOINT-CAPTURE=~s~%" (nreverse rows)))))))
        """)
        assert "COPY-SAVEPOINT-CAPTURE=(11 13 21 23)" in result, result
        assert connection.query("SELECT id FROM savepoint_capture ORDER BY id") == [
            ["11"], ["13"], ["21"], ["23"]
        ]
        connection.query("SELECT pg_drop_replication_slot('savepoint_capture_slot')")


if __name__ == "__main__":
    server = OwnedLisp()
    try:
        server.start()
        verify_existing_rows_validation(server)
        verify_parent_changes(server)
        verify_empty_cascade_does_not_admit_grandchild(server)
        verify_batched_insert_savepoint(server)
        verify_batched_cascade_savepoint(server)
        verify_batched_cascade_rechecks_updated_row(server)
        verify_probe_skips_deleted_versions(server)
        verify_child_parent_races(server)
        verify_composite_self_reference(server)
        verify_inline_create_reference(server)
        verify_referential_actions(server)
        verify_copy_foreign_keys(server)
        verify_deferred_constraints(server)
        verify_deferred_row_versions(server)
        verify_partman_deferred_cascade(server)
        verify_savepoint_heap_undo(server)
        verify_failed_savepoint_recovers(server)
        verify_savepoint_wakes_row_waiters(server)
        verify_savepoint_logical_capture(server)
        verify_before_claim_rechecks_latest_version(server)
        verify_recovered_constraint(server)
    finally:
        server.stop()
        shutil.rmtree(server.root)
