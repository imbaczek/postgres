"""Check numeric literal LIMIT coercion against a wire server."""

import importlib.util
from pathlib import Path
import sys

spec = importlib.util.spec_from_file_location(
    "command_complete", Path(__file__).with_name("command-complete.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)
from parking_wire import WireError


def check(connection):
    source = "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) t"
    for order in ("", " ORDER BY x DESC"):
        values = ["1", "2", "3"] if not order else ["3", "2", "1"]
        for literal, count in [("1.5", 2), ("2.5", 3), (".5", 1),
                               ("1.49", 1), ("0.49", 0), ("-0.49", 0),
                               ("+1.5", 2), ("1e0", 1)]:
            sql = source + order + " LIMIT " + literal
            assert connection.query(sql) == [[v] for v in values[:count]], sql
    for literal, state in [("-0.5", "2201W"), ("-1", "2201W"),
                            ("9223372036854775807.5", "22003")]:
        try:
            connection.query(source + " LIMIT " + literal)
        except WireError as error:
            assert error.sqlstate == state, (literal, error)
        else:
            raise AssertionError(f"LIMIT {literal} succeeded")
    assert connection.query("SELECT 1") == [["1"]]


def run():
    server = fixture.OwnedLisp() if len(sys.argv) == 1 else None
    connection = None
    try:
        if server is not None:
            server.start()
        connection = fixture.Connection(server.port if server is not None else int(sys.argv[1]))
        check(connection)
        print("limit-numeric: PASS")
    finally:
        if connection is not None:
            connection.close()
        if server is not None:
            server.stop()
            print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()
