"""Sequence literal resolution and NULL strictness through both wire protocols."""
import importlib.util
from pathlib import Path
import re
import struct
import sys

spec = importlib.util.spec_from_file_location(
    "command_complete", Path(__file__).with_name("command-complete.py"))
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)


def extended(connection, sql):
    # Native Extended Query requires strings as parameters. Their declared text
    # type intentionally exercises late lookup, rather than literal regclass input.
    parameters = []

    def parameter(match):
        parameters.append(match[0][1:-1].encode())
        return "$" + str(len(parameters))

    sql = re.sub(r"'binding_[a-z]+'", parameter, sql)
    connection.socket.sendall(wire.parse(sql, (25,) * len(parameters)) +
                              wire.bind(tuple(parameters)) +
                              wire.frame(b"E", b"\0" + struct.pack("!I", 0)) +
                              wire.frame(b"S", b""))
    rows, error, parsed = [], None, False
    while True:
        kind, body = connection.receive()
        if kind == b"1":
            parsed = True
        elif kind == b"D":
            rows.append(connection.data_row(body))
        elif kind == b"E":
            error = connection.error_fields(body)["C"]
        elif kind == b"Z":
            return (error if error else rows), parsed


def check(connection):
    connection.query("DROP TABLE IF EXISTS binding_table")
    connection.query("DROP SEQUENCE IF EXISTS binding_seq")
    connection.query("CREATE SEQUENCE binding_seq")
    connection.query("CREATE TABLE binding_table(id int)")
    cases = (
        ("SELECT nextval(NULL),currval(NULL)", [[None, None]], False),
        ("SELECT nextval('binding_absent')", "42P01", True),
        ("SELECT currval('binding_absent')", "42P01", True),
        ("SELECT setval('binding_absent',NULL::bigint)", "42P01", True),
        ("SELECT setval('binding_absent',NULL)", "42P01", True),
        ("SELECT setval('binding_absent',1,NULL)", "42P01", True),
        ("SELECT CASE WHEN false THEN nextval('binding_absent') ELSE 7 END", "42P01", True),
        ("SELECT 1 HAVING false AND nextval('binding_absent')>0", "42P01", True),
        ("SELECT 1 HAVING true OR currval('binding_absent')>0", "42P01", True),
        ("SELECT CASE WHEN false THEN nextval('binding_absent'::text) ELSE 7 END", [["7"]], False),
        ("SELECT 1 HAVING false AND nextval('binding_absent'::text)>0", [], False),
        ("SELECT 1 HAVING true OR currval('binding_absent'::text)>0", [["1"]], False),
        ("SELECT setval('binding_absent'::text,NULL::bigint)", [[None]], False),
        ("SELECT nextval('binding_absent'::text)", "42P01", False),
        ("SELECT CASE WHEN false THEN nextval('binding_table') ELSE 7 END", [["7"]], False),
        ("SELECT nextval('binding_table')", "42809", False),
        ("SELECT nextval(DISTINCT 'binding_absent')", "42809", True),
        ("SELECT nextval('binding_absent' ORDER BY 1)", "42809", True),
        ("SELECT CASE WHEN false THEN nextval('123456789') ELSE 7 END", [["7"]], True),
        ("SELECT CASE WHEN false THEN currval('-') ELSE 7 END", [["7"]], True),
        ("SELECT setval('123456789',NULL::bigint)", [[None]], True),
        ("SELECT CASE WHEN false THEN nextval('4294967296') ELSE 7 END", "22003", True),
        ("SELECT setval('0008',NULL::bigint)", "22P02", True),
        ("SELECT setval('binding_seq',37,true)", [["37"]], False),
        ("SELECT setval('binding_seq',NULL::bigint),setval('binding_seq',99,NULL)", [[None, None]], False),
        ("SELECT currval('binding_seq'),nextval('binding_seq')", [["37", "38"]], False),
        ("SELECT setval('binding_seq',55,false)", [["55"]], False),
        ("SELECT setval('binding_seq',NULL)", [[None]], False),
        ("SELECT nextval('binding_seq')", [["55"]], False),
        ("SELECT 42", [["42"]], False),
    )
    for sql, expected, _ in cases:
        outcome = connection.observe(sql)
        actual = outcome[1] if outcome[0] == "error" else outcome[2]
        assert actual == expected, ("simple", sql, actual, expected)
    late_cases = [(sql, expected) for sql, expected, early in cases if not early] + [
        ("SELECT nextval('binding_absent')", "42P01"),
        ("SELECT currval('binding_absent')", "42P01"),
        ("SELECT setval('binding_absent',NULL)", [[None]]),
        ("SELECT CASE WHEN false THEN nextval('binding_absent') ELSE 7 END", [["7"]]),
        ("SELECT 1 HAVING false AND nextval('binding_absent')>0", []),
    ]
    for sql, expected in late_cases:
        actual, parsed = extended(connection, sql)
        assert parsed, ("extended", sql, "text parameter rejected at Parse")
        assert actual == expected, ("extended", sql, actual, expected)
    print(f"sequence-name-binding: {len(cases)} Simple literal/NULL and "
          f"{len(late_cases)} Extended text-parameter checks PASS")


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
