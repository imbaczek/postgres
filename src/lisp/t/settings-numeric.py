"""Verify decimal SET tokens through session state on the wire."""

import importlib.util
from pathlib import Path
import sys

spec = importlib.util.spec_from_file_location(
    "command_complete", Path(__file__).with_name("command-complete.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


def check(connection):
    original_timeout = connection.query("SHOW statement_timeout")[0][0]
    original_name = connection.query("SHOW application_name")[0][0]
    try:
        for value, expected in [("1.5", "2ms"), ("'1.5'", "2ms"),
                                ("+1.5", "2ms"), (".5", "0"),
                                ("2.5", "2ms"), ("1e1", "10ms")]:
            connection.query(f"SET statement_timeout TO {value}")
            assert connection.query("SHOW statement_timeout") == [[expected]], value
            connection.query("SET statement_timeout TO 0")
        for value, expected in [("1.5", "1.5"), ("'1.5'", "1.5"),
                                ("-1.5", "-1.5"), ("+1.5", "1.5")]:
            connection.query(f"SET application_name TO {value}")
            assert connection.query("SHOW application_name") == [[expected]], value
    finally:
        connection.query("SET statement_timeout TO '" + original_timeout.replace("'", "''") + "'")
        connection.query("SET application_name TO '" + original_name.replace("'", "''") + "'")


def run():
    server = fixture.OwnedLisp() if len(sys.argv) == 1 else None
    connection = None
    try:
        if server is not None:
            server.start()
        connection = fixture.Connection(server.port if server is not None else int(sys.argv[1]))
        check(connection)
        print("settings-numeric: PASS")
    finally:
        if connection is not None:
            connection.close()
        if server is not None:
            server.stop()
            print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()
