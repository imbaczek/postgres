"""TRUNCATE rejects this session's live executors before replacing storage.

Use a multi-page heap so a suspended scan cannot hide the loss in its page copy.
"""
import importlib.util
from pathlib import Path
import struct

spec = importlib.util.spec_from_file_location(
    "portal_fixture", Path(__file__).with_name("portal-snapshot.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


class Connection(fixture.Connection):
    def prepare_sql(self, sql, statement):
        self.send(b"P", statement.encode() + b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
        self.bind(statement=statement)
        self.send(b"H")
        self.through(b"2")

    def execute_limit(self, limit, portal="portal"):
        self.send(b"E", portal.encode() + b"\0" + struct.pack("!I", limit))
        self.send(b"S")
        frames, rows = [], []
        while True:
            kind, body = self.receive()
            if kind == b"D":
                rows.append(self.data_row(body))
            elif kind == b"E":
                raise AssertionError(self.error_fields(body))
            else:
                frames.append((kind, body))
            if kind == b"Z":
                return rows, frames

    def truncate_error(self):
        self.send(b"Q", b"TRUNCATE portal_truncate\0")
        frames = []
        while True:
            kind, body = self.receive()
            frames.append((kind, body))
            if kind == b"Z":
                break
        assert [kind for kind, _ in frames] == [b"E", b"Z"], frames
        error = self.error_fields(frames[0][1])
        assert error["C"] == "55006", error
        assert "active queries in this session" in error["M"], error
        assert frames[-1] == (b"Z", b"E"), frames


def run():
    server = fixture.wire.OwnedLisp()
    connection = None
    try:
        server.start()
        connection = Connection(server.port)
        connection.query("CREATE TABLE portal_truncate(id int)")
        connection.query("INSERT INTO portal_truncate VALUES " +
                         ",".join(f"({i})" for i in range(1, 1001)))
        # More than one heap page is an explicit precondition of the regression.
        server.control(
            '(let ((pg::*backend* (pg::make-backend))) '
            '(assert (> (pg::smgr-nblocks (pg::pg-class-relfilenode '
            '(pg::resolve-table-or-error "portal_truncate"))) 1)))')

        connection.query("BEGIN")
        connection.prepare("portal_truncate", statement="suspended")
        rows, frames = connection.execute_limit(1)
        assert rows == [["1"]] and frames == [(b"s", b""), (b"Z", b"T")], (rows, frames)
        connection.truncate_error()
        connection.query("ROLLBACK")
        assert connection.query("SELECT count(*) FROM portal_truncate") == [["1000"]]

        # Bind has already initialized the executor even before its first pull.
        connection.query("BEGIN")
        connection.prepare("portal_truncate", statement="bound")
        connection.truncate_error()
        connection.query("ROLLBACK")

        # Relation uses include nested plans and index-backed scans, even when
        # their physical scan is reached only on a subsequent Execute pull.
        connection.query("CREATE INDEX portal_truncate_id ON portal_truncate(id)")
        for number, sql in enumerate((
                "SELECT q.id FROM (SELECT id FROM portal_truncate) q",
                "WITH q AS (SELECT id FROM portal_truncate) SELECT id FROM q",
                "SELECT a.id FROM portal_truncate a JOIN portal_truncate b ON a.id=b.id",
                "SELECT id FROM portal_truncate WHERE id > 0")):
            connection.query("BEGIN")
            connection.prepare_sql(sql, f"nested{number}")
            assert connection.execute_limit(1)[0] == [["1"]]
            connection.truncate_error()
            connection.query("ROLLBACK")

        # Like C's queryDesc, a completed named portal still retains executor
        # references until Close; transaction-level reader retention is separate.
        connection.query("BEGIN")
        connection.prepare("portal_truncate", statement="completed")
        assert len(connection.execute_limit(0)[0]) == 1000
        connection.truncate_error()
        connection.query("ROLLBACK")

        # Closing a suspended portal releases executable uses, while its reader
        # registrations remain held by the transaction. Those do not block us.
        connection.query("BEGIN")
        connection.prepare("portal_truncate", statement="closed")
        assert connection.execute_limit(1)[0] == [["1"]]
        connection.close_portal()
        connection.query("TRUNCATE portal_truncate")
        assert connection.query("SELECT count(*) FROM portal_truncate") == [["0"]]
        connection.query("ROLLBACK")

        # A completed Simple Query leaves transaction locks but no live executor.
        connection.query("BEGIN")
        assert len(connection.query("SELECT id FROM portal_truncate")) == 1000
        connection.query("TRUNCATE portal_truncate")
        connection.query("ROLLBACK")
        assert connection.query("SELECT count(*) FROM portal_truncate") == [["1000"]]

        # A portal referencing another relation must not cause a false positive.
        connection.query("CREATE TABLE portal_other(id int)")
        connection.query("INSERT INTO portal_other VALUES(7),(8)")
        connection.query("BEGIN")
        connection.prepare("portal_other", statement="unrelated")
        assert connection.execute_limit(1)[0] == [["7"]]
        connection.query("TRUNCATE portal_truncate")
        assert connection.execute_limit(0) == ([["8"]], [(b"C", b"SELECT 1\0"), (b"Z", b"T")])
        connection.query("ROLLBACK")
        print("truncate-portal: PASS (multi-page suspension, bound/closed portals, completed query)")
    finally:
        if connection is not None:
            connection.close()
        server.stop()
        print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()
