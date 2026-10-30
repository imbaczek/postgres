"""TX-04: autocommit set_config changes persist only after successful execution."""

import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "command_complete", Path(__file__).with_name("command-complete.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)
from parking_wire import WireError


class Connection(fixture.Connection):
    def receive(self):
        kind, body = super().receive()
        if kind == b"Z":
            self.status = body
        return kind, body


def expect_error(connection, sql, status=b"I"):
    try:
        connection.query(sql)
    except WireError as error:
        assert error.sqlstate == "22012", error
    else:
        raise AssertionError("runtime division error was not raised")
    assert connection.status == status, connection.status


def check(connection):
    connection.query("CREATE TABLE settings_divisor(divisor int)")
    connection.query("INSERT INTO settings_divisor VALUES(0)")
    # The table column prevents planning-time constant folding of the error.
    connection.query("SET statement_timeout=0")
    expect_error(connection,
                 "SELECT set_config('statement_timeout','1234ms',false), "
                 "1/divisor FROM settings_divisor")
    assert connection.query("SHOW statement_timeout") == [["0"]]
    assert connection.query("SELECT set_config('statement_timeout','1234ms',false)") == [["1234ms"]]
    assert connection.query("SHOW statement_timeout") == [["1234ms"]]
    expect_error(connection,
                 "SELECT set_config('statement_timeout',NULL,false), "
                 "1/divisor FROM settings_divisor")
    assert connection.query("SHOW statement_timeout") == [["1234ms"]]
    connection.query("SET statement_timeout=0")
    connection.query("SELECT set_config('regression.setting','before',false)")
    expect_error(connection,
                 "SELECT set_config('regression.setting','after',false), "
                 "1/divisor FROM settings_divisor")
    assert connection.query("SELECT current_setting('regression.setting')") == [["before"]]
    expect_error(connection,
                 "SELECT set_config('regression.setting',NULL,false), "
                 "1/divisor FROM settings_divisor")
    assert connection.query("SELECT current_setting('regression.setting')") == [["before"]]
    # Repeated mutations keep the original undo value, including reset then SET.
    expect_error(connection,
                 "SELECT set_config('regression.setting',NULL,false), "
                 "set_config('regression.setting','last',false), "
                 "1/divisor FROM settings_divisor")
    assert connection.query("SELECT current_setting('regression.setting')") == [["before"]]
    connection.query("BEGIN")
    connection.query("SELECT set_config('regression.setting','explicit',false)")
    connection.query("ROLLBACK")
    assert connection.query("SELECT current_setting('regression.setting')") == [["before"]]
    connection.query("SELECT set_config('regression.setting',NULL,false)")
    assert connection.query("SELECT current_setting('regression.setting')") == [[""]]
    assert connection.query("SELECT 1") == [["1"]]


def run():
    server = fixture.OwnedLisp()
    connection = None
    try:
        server.start()
        connection = Connection(server.port)
        check(connection)
        print("settings-rollback: PASS (autocommit error, success, resets, custom settings, explicit rollback)")
    finally:
        if connection is not None:
            connection.close()
        server.stop()
        print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()
