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


def run():
    server = wire.OwnedLisp()
    writer = reader = None
    try:
        server.start()
        writer, reader = Connection(server.port), Connection(server.port)
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
