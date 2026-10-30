"""Check numeric literal LIMIT coercion against a wire server."""

import importlib.util
from pathlib import Path
import sys

spec = importlib.util.spec_from_file_location(
    "command_complete", Path(__file__).with_name("command-complete.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)
from parking_wire import WireError


def extended(connection, sql):
    connection.socket.sendall(fixture.parse(sql) + fixture.bind() +
                              fixture.frame(b"E", b"\0\0\0\0\0") + fixture.frame(b"S", b""))
    rows, failure = [], None
    while True:
        kind, body = connection.receive()
        if kind == b"D":
            rows.append(connection.data_row(body))
        elif kind == b"E":
            failure = WireError(connection.error_fields(body))
        elif kind == b"Z":
            if failure:
                raise failure
            return rows


def check(connection, query=None):
    query = query or connection.query
    source = "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) t"
    for order in ("", " ORDER BY x DESC"):
        values = ["1", "2", "3"] if not order else ["3", "2", "1"]
        for literal, count in [("1.5", 2), ("2.5", 3), (".5", 1),
                               ("1.49", 1), ("0.49", 0), ("-0.49", 0),
                               ("+1.5", 2), ("1e0", 1)]:
            sql = source + order + " LIMIT " + literal
            assert query(sql) == [[v] for v in values[:count]], sql
    errors = [(source + " LIMIT " + literal, state)
              for literal, state in [("-0.5", "2201W"), ("-1", "2201W"),
                                      ("9223372036854775807.5", "22003")]]
    errors += [("SELECT missing LIMIT -1", "42703"),
               ("SELECT missing LIMIT 9223372036854775808", "42703"),
               ("SELECT 1 LIMIT -1 WHERE false", "42601"),
               ("SELECT 1 LIMIT 9223372036854775808 WHERE false", "42601"),
               (source + " ORDER BY x LIMIT -0.5", "2201W"),
               ("SELECT 1 AS x,pg_get_publication_tables(VARIADIC ARRAY[]::text[]) "
                "ORDER BY x LIMIT -0.5", "2201W")]
    for sql, state in errors:
        try:
            query(sql)
        except WireError as error:
            assert error.sqlstate == state, (sql, error)
        else:
            raise AssertionError(f"{sql} succeeded")
    assert query("WITH unused AS(SELECT 1 LIMIT -1) SELECT 7") == [["7"]]
    assert query("WITH unused AS(SELECT 1 LIMIT 9223372036854775808) SELECT 7") == [["7"]]
    assert query("SELECT CASE WHEN false THEN "
                 "(SELECT 1 LIMIT 9223372036854775808) ELSE 7 END") == [["7"]]
    assert query("SELECT * FROM (SELECT 1 LIMIT -1) q LIMIT 0") == []
    assert query("SELECT 1") == [["1"]]


def run():
    server = fixture.OwnedLisp() if len(sys.argv) == 1 else None
    connection = None
    try:
        if server is not None:
            server.start()
        connection = fixture.Connection(server.port if server is not None else int(sys.argv[1]))
        check(connection)
        check(connection, lambda sql: extended(connection, sql))
        print("limit-numeric: PASS")
    finally:
        if connection is not None:
            connection.close()
        if server is not None:
            server.stop()
            print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()
