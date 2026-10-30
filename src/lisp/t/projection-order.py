"""Regression: Simple and Extended Query share post-sort volatile projection."""
import importlib.util
from pathlib import Path
import struct
import sys

spec = importlib.util.spec_from_file_location(
    "command_complete", Path(__file__).with_name("command-complete.py"))
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)


def extended(connection, sql):
    parameters = []
    for name in ("projection_seq", "projection_key"):
        literal = "'" + name + "'"
        if literal in sql:
            parameters.append(name.encode())
            sql = sql.replace(literal, "$" + str(len(parameters)))
    connection.socket.sendall(wire.parse(sql, (25,) * len(parameters)) +
                              wire.bind(tuple(parameters)) +
                              wire.frame(b"E", b"\0" + struct.pack("!I", 0)) +
                              wire.frame(b"S", b""))
    rows = []
    while True:
        kind, body = connection.receive()
        assert kind != b"E", connection.error_fields(body)
        if kind == b"D":
            rows.append(connection.data_row(body))
        elif kind == b"Z":
            return rows


def check(connection):
    connection.query("CREATE TABLE projection_order(id int)")
    connection.query("INSERT INTO projection_order VALUES (2),(1)")
    connection.query("CREATE SEQUENCE projection_seq")
    connection.query("CREATE SEQUENCE projection_key")
    cases = (
        ("SELECT id,nextval('projection_seq') FROM projection_order ORDER BY id",
         [["1", "1"], ["2", "2"]], "2"),
        ("SELECT id,nextval('projection_seq') FROM projection_order ORDER BY id LIMIT 1",
         [["1", "1"]], "1"),
        ("SELECT id,nextval('projection_seq') FROM projection_order ORDER BY id DESC LIMIT 1",
         [["2", "1"]], "1"),
        ("SELECT id,nextval('projection_seq') AS n FROM projection_order ORDER BY n DESC LIMIT 1",
         [["1", "2"]], "2"),
        ("SELECT id,nextval('projection_seq') FROM projection_order ORDER BY 2 DESC LIMIT 1",
         [["1", "2"]], "2"),
        ("SELECT id,nextval('projection_seq') FROM projection_order ORDER BY nextval('projection_key') DESC LIMIT 1",
         [["1", "1"]], "1"),
        ("SELECT DISTINCT id,nextval('projection_seq') FROM projection_order ORDER BY id",
         [["1", "2"], ["2", "1"]], "2"),
        ("SELECT id,nextval('projection_seq') FROM projection_order GROUP BY id ORDER BY id LIMIT 1",
         [["1", "1"]], "1"),
        ("SELECT id,nextval('projection_seq') FROM (SELECT id FROM projection_order ORDER BY id) u ORDER BY id DESC LIMIT 1",
         [["2", "1"]], "1"),
        ("SELECT id,nextval('projection_seq') FROM projection_order ORDER BY id LIMIT 0",
         [], "100"),
    )
    for query in (connection.query, lambda sql: extended(connection, sql)):
        for sql, expected, current in cases:
            # Called=true makes LIMIT 0 observable without currval being undefined.
            connection.query("SELECT setval('projection_seq',100,true)")
            connection.query("SELECT setval('projection_key',1,false)")
            if current != "100":
                connection.query("SELECT setval('projection_seq',1,false)")
            actual = query(sql)
            assert actual == expected, (sql, actual, expected)
            assert connection.query("SELECT currval('projection_seq')") == [[current]], sql
            if "ORDER BY nextval(" in sql:
                assert connection.query("SELECT currval('projection_key')") == [["2"]], sql
    print("projection-order: Simple/Extended sort, top-N, aliases, ordinals and sequence effects PASS")


def run():
    server = None if len(sys.argv) > 1 else wire.OwnedLisp()
    connection = None
    try:
        if server:
            server.start()
        connection = wire.Connection(int(sys.argv[1]) if server is None else server.port)
        check(connection)
    finally:
        if connection:
            connection.close()
        if server:
            server.stop()
            print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()
