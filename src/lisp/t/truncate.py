"""TX-02: whole-relation TRUNCATE, rollback, exclusion, and crash recovery."""
import importlib.util
from pathlib import Path
import random
import string

spec = importlib.util.spec_from_file_location(
    "transaction_fixture", Path(__file__).with_name("command-complete.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)
from parking_wire import WireError


def expect_timeout(connection, sql):
    try:
        connection.query(sql)
    except WireError as error:
        assert error.sqlstate == "57014", (sql, error)
    else:
        raise AssertionError(f"conflicting access completed: {sql}")


def ddl_publication_race(server, truncater, builder):
    """Publish an index after TRUNCATE captures its storage set, before admission."""
    builder.query("CREATE TABLE truncate_ddl(id int PRIMARY KEY, value int)")
    builder.query("INSERT INTO truncate_ddl VALUES(11,101)")
    marker = server.root / "truncate-admission-paused"
    server.control("(defvar pg::*truncate-test-acquire* "
                   "(symbol-function 'pg::acquire-transaction-relation-locks))")
    server.control("(defvar pg::*truncate-test-resume* (sb-thread:make-semaphore))")
    try:
        server.control(
            "(setf (symbol-function 'pg::acquire-transaction-relation-locks) "
            "(let ((first t)) (lambda (nodes) (when first (setf first nil) "
            f'(with-open-file (marker "{marker}" :direction :output) '
            "(write-line \"paused\" marker)) "
            "(sb-thread:wait-on-semaphore pg::*truncate-test-resume*)) "
            "(funcall pg::*truncate-test-acquire* nodes))))")
        truncater.send(b"Q", b"TRUNCATE truncate_ddl\0")
        fixture.wait_for_file(marker, server.process)
        builder.query("CREATE INDEX truncate_ddl_value ON truncate_ddl(value)")
        server.control("(sb-thread:signal-semaphore pg::*truncate-test-resume*)")
        frames = []
        while True:
            kind, body = truncater.receive()
            frames.append((kind, body))
            if kind == b"Z":
                break
        assert frames == [(b"C", b"TRUNCATE TABLE\0"), (b"Z", b"I")], frames
        # Check the new index itself, since a heap qualification could hide
        # stale index entries from an ordinary SELECT.
        server.control(
            '(let* ((pg::*backend* (pg::make-backend)) '
            '(index (pg::lookup-index "truncate_ddl_value"))) '
            '(assert (null (pg::btree-search (pg::pg-index-relfilenode index) '
            "(pg::encode-index-key (list 101) '(:int4))))))")
        builder.query("INSERT INTO truncate_ddl VALUES(12,101)")
        assert builder.query("SELECT id FROM truncate_ddl WHERE value=101") == [["12"]]
    finally:
        server.control("(setf (symbol-function 'pg::acquire-transaction-relation-locks) "
                       "pg::*truncate-test-acquire*)")
        server.control("(sb-thread:signal-semaphore pg::*truncate-test-resume*)")
    builder.query("DROP TABLE truncate_ddl")


def run():
    server = fixture.OwnedLisp()
    connections = []
    def connect():
        connection = fixture.Connection(server.port)
        connections.append(connection)
        return connection
    try:
        server.start()
        a, b, witness = connect(), connect(), connect()
        b.query("CREATE TABLE truncate_rows(id int PRIMARY KEY, payload text)")
        b.query("CREATE TABLE truncate_horizon(id int)")
        a.query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        assert a.query("SELECT id FROM truncate_rows") == []
        b.query("INSERT INTO truncate_rows VALUES(1,'one')")
        a.query("TRUNCATE truncate_rows")
        a.query("COMMIT")
        assert b.query("SELECT id FROM truncate_rows") == [], "retained snapshot left a row"
        ddl_publication_race(server, a, b)

        # An old snapshot that had not accessed this table sees empty storage,
        # including tuple versions deleted before TRUNCATE began.
        b.query("INSERT INTO truncate_rows VALUES(2,'two'),(3,'three')")
        a.query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        a.query("SELECT id FROM truncate_horizon")
        b.query("DELETE FROM truncate_rows WHERE id=2")
        b.query("TRUNCATE truncate_rows")
        assert a.query("SELECT id FROM truncate_rows") == []
        a.query("ROLLBACK")

        payload = ''.join(random.Random(91).choices(string.ascii_letters, k=20000))
        b.query(f"INSERT INTO truncate_rows VALUES(4,'{payload}'),(5,'old')")
        b.query("UPDATE truncate_rows SET payload='updated' WHERE id=5")
        b.query("DELETE FROM truncate_rows WHERE id=5")
        server.control("(setf pg::*wal-dirty-resident-limit-pages* 0)")
        a.query("BEGIN")
        a.query("TRUNCATE truncate_rows")
        assert a.query("SELECT id FROM truncate_rows") == []
        a.query("INSERT INTO truncate_rows VALUES(4,'replacement')")
        assert a.query("SELECT payload FROM truncate_rows WHERE id=4") == [["replacement"]]
        # Force an unrelated durable boundary while replacement pages are private.
        witness.query("INSERT INTO truncate_horizon VALUES(1)")
        witness.query("CHECKPOINT")
        a.query("ROLLBACK")
        assert b.query("SELECT id,payload FROM truncate_rows") == [["4", payload]]

        # Existing readers and writers prevent whole-relation replacement.
        a.query("SET statement_timeout='250ms'")
        b.query("BEGIN")
        b.query("INSERT INTO truncate_rows VALUES(6,'pending')")
        expect_timeout(a, "TRUNCATE truncate_rows")
        b.query("COMMIT")
        b.query("BEGIN")
        b.query("SELECT id FROM truncate_rows")
        expect_timeout(a, "TRUNCATE truncate_rows")
        b.query("ROLLBACK")
        a.query("SET statement_timeout=0")

        # New readers/writers remain excluded after the statement returns.
        a.query("BEGIN")
        a.query("TRUNCATE truncate_rows")
        b.query("SET statement_timeout='250ms'")
        expect_timeout(b, "SELECT id FROM truncate_rows")
        expect_timeout(b, "INSERT INTO truncate_rows VALUES(7,'blocked')")
        b.query("SET statement_timeout=0")
        a.query("INSERT INTO truncate_rows VALUES(8,'kept')")
        a.query("TRUNCATE truncate_rows")
        a.query("INSERT INTO truncate_rows VALUES(9,'final')")
        a.query("COMMIT")
        assert b.query("SELECT id,payload FROM truncate_rows") == [["9", "final"]]
        assert b.query("SELECT payload FROM truncate_rows WHERE id=9") == [["final"]]
        b.query("VACUUM truncate_rows")
        assert b.query("SELECT payload FROM truncate_rows WHERE id=9") == [["final"]]

        # Crash while an uncommitted replacement has survived CHECKPOINT.
        a.query("BEGIN")
        a.query("TRUNCATE truncate_rows")
        witness.query("CHECKPOINT")
        server.stop(immediate=True)
        for connection in connections:
            connection.close()
        connections.clear()
        server.start()
        b = connect()
        assert b.query("SELECT id,payload FROM truncate_rows") == [["9", "final"]]
        # Replacement page images without a complete commit terminal must not
        # erase the old storage during recovery.
        server.control('(setf pg::*wal-test-pause-at* "after-page-images")')
        b.send(b"Q", b"TRUNCATE truncate_rows\0")
        fixture.wait_for_file(server.data / "pg_wal/test-paused-after-page-images",
                              server.process)
        server.stop(immediate=True)
        b.close()
        connections.clear()
        server.start()
        b = connect()
        assert b.query("SELECT id,payload FROM truncate_rows") == [["9", "final"]]
        b.query("TRUNCATE truncate_rows")
        server.stop(immediate=True)
        b.close()
        connections.clear()
        server.start()
        b = connect()
        assert b.query("SELECT id FROM truncate_rows") == []
        b.query("INSERT INTO truncate_rows VALUES(9,'reused')")
        assert b.query("SELECT payload FROM truncate_rows WHERE id=9") == [["reused"]]
        print("truncate: PASS (snapshots, rollback, indexes/TOAST, exclusion, recovery)")
    finally:
        resume = server.data / "pg_wal/test-resume-after-page-images"
        if resume.parent.exists():
            resume.touch()
        for connection in connections:
            connection.close()
        server.stop()
        print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()
