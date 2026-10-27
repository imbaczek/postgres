"""Compare application_name SQL, startup and wire behavior with pinned C."""
import json
import socket
import struct
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg2

from parking_wire import RawConnection, WireError


class ObservedConnection(RawConnection):
    def __init__(self, port):
        self.names = []
        self.notices = []
        self.backend_pid = None
        self.statuses = []
        self.frames = []
        self.errors = []
        super().__init__(port, "startup")

    def receive(self):
        kind, body = super().receive()
        if kind == b"S":
            name, value, _ = body.decode().split("\0")
            if name == "application_name":
                self.names.append(value)
            if name in ("application_name", "TimeZone"):
                self.statuses.append([name, value])
        elif kind == b"N":
            fields = self.error_fields(body)
            self.notices.append([fields["V"], fields["C"], fields["M"]])
        elif kind == b"K":
            self.backend_pid = struct.unpack("!II", body)[0]
        elif kind == b"E":
            self.errors.append(self.error_fields(body)["C"])
        if kind == b"C":
            self.frames.append(["C", body.rstrip(b"\0").decode()])
        elif kind in (b"D", b"E", b"Z"):
            self.frames.append([kind.decode()])
        return kind, body

    def query(self, sql):
        self.names.clear()
        self.notices.clear()
        self.statuses.clear()
        self.frames.clear()
        self.errors.clear()
        return super().query(sql)

    def observation(self):
        return dict(reports=list(self.names), statuses=sorted(self.statuses),
                    frames=list(self.frames), notices=list(self.notices))


def control(base, root, sbcl, expression):
    result = subprocess.run([sbcl, "--script", str(base / "t/control-client.lisp"),
                             "unix", str(root / "native.sock")], input=expression + "\n(quit)\n",
                            text=True, capture_output=True, timeout=30, check=True)
    assert ";; error:" not in result.stdout + result.stderr, result.stdout + result.stderr
    return result.stdout


def startup_error(port):
    wire = RawConnection.__new__(RawConnection)
    wire.socket = socket.create_connection(("127.0.0.1", port), timeout=5)
    values = ("user", "postgres", "database", "postgres", "options",
              "-c application_name=" + "x" * 100 + " -c statement_timeout=bad")
    body = struct.pack("!I", 196608) + "\0".join(values).encode() + b"\0\0"
    transcript = []
    try:
        wire.socket.sendall(struct.pack("!I", len(body) + 4) + body)
        while True:
            kind, body = wire.receive()
            if kind == b"R":
                transcript.append(["authentication", struct.unpack("!I", body)[0]])
            elif kind in (b"N", b"E"):
                fields = wire.error_fields(body)
                transcript.append([kind.decode(), fields["C"], fields["M"]])
                if kind == b"E":
                    assert fields["C"] == "22023"
                    return transcript
    finally:
        wire.close()


def packet_cancel(port, native):
    blocker = psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres", connect_timeout=3)
    admin = psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres", connect_timeout=3)
    admin.autocommit = True
    wire = ObservedConnection(port)
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        with blocker.cursor() as cursor:
            cursor.execute("UPDATE application_name_zero SET id=0 WHERE id=0")
        wire.names.clear()
        future = pool.submit(wire.query, "SET application_name='cancel-packet'; "
                             "INSERT INTO application_name_zero VALUES (9); "
                             "UPDATE application_name_zero SET id=1 WHERE id=0")
        deadline = time.monotonic() + 25
        while True:
            if future.done():
                raise AssertionError(("packet did not wait for row lock", future.result()))
            if native:
                expression = (f'(let ((backend (lookup-backend {wire.backend_pid}))) '
                              '(format t "APP_WAIT=~a~%" '
                              '(and backend (backend-waiting-xid backend) '
                              '(equal "cancel-packet" (backend-application-name backend)))))')
                waiting = "APP_WAIT=T" in control(*native, expression)
            else:
                with admin.cursor() as cursor:
                    cursor.execute("SELECT application_name,wait_event_type FROM pg_stat_activity WHERE pid=%s",
                                   (wire.backend_pid,))
                    waiting = cursor.fetchone() == ("cancel-packet", "Lock")
            if waiting:
                break
            assert time.monotonic() < deadline, "packet did not expose its lock wait"
            time.sleep(0.025)
        with admin.cursor() as cursor:
            cursor.execute("SELECT pg_cancel_backend(%s)", (wire.backend_pid,))
            assert cursor.fetchone() == (True,)
        try:
            future.result(timeout=10)
            raise AssertionError("cancelled packet unexpectedly succeeded")
        except WireError as failure:
            assert failure.sqlstate == "57014"
        reports = list(wire.names)
        blocker.rollback()
        return dict(reports=reports, current=wire.query("SHOW application_name"),
                    rows=wire.query("SELECT id FROM application_name_zero ORDER BY id"))
    finally:
        blocker.rollback()
        wire.close()
        pool.shutdown(wait=True)
        admin.close()
        blocker.close()


def observe(port, native=None):
    result = {}
    result["startup_error"] = startup_error(port)
    for label, options in (
        ("default", {}),
        ("option", dict(options="-c application_name=option")),
        ("field", dict(options="-c application_name=option", application_name="startup")),
        ("expanded_field", dict(application_name="é" * 32)),
        ("unicode_option", dict(options="-c application_name=é")),
    ):
        connection = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                      dbname="postgres", connect_timeout=3, **options)
        connection.autocommit = True
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT setting,source,context,vartype FROM pg_settings WHERE name='application_name'")
                result[label] = [cursor.fetchall(), connection.get_parameter_status("application_name")]
                cursor.execute("SET application_name='changed'; RESET application_name")
                cursor.execute("SHOW application_name")
                result[label + "_reset"] = [cursor.fetchall(), connection.get_parameter_status("application_name")]
        finally:
            connection.close()
    wire = ObservedConnection(port)
    try:
        result["startup_wire"] = list(wire.names)
        steps = (
            ("set", "SET application_name='session'"),
            ("begin", "BEGIN"),
            ("local1", "SET LOCAL application_name='local1'"),
            ("local2", "SET LOCAL application_name='local2'"),
            ("local_commit", "COMMIT"),
            ("implicit_local", "SELECT set_config('application_name','transient',true)"),
            ("local_outside", "SET LOCAL application_name='ignored'"),
            ("local_outside_long", "SET LOCAL application_name='" + "x" * 100 + "'"),
            ("coalesced", "SET application_name='hidden'; SET application_name='visible'"),
            ("rollback", "BEGIN; SET application_name='rolled-back'; ROLLBACK"),
            ("session_after_local", "BEGIN; SET LOCAL application_name='local'; SET application_name='wins'; COMMIT"),
            ("reset_local", "BEGIN; SET LOCAL application_name='local'; RESET application_name; COMMIT"),
            ("reset_all", "SET application_name='reset-all'; RESET ALL"),
            ("null_reset", "SELECT set_config('application_name',NULL,false)"),
            ("escaped", "SELECT set_config('application_name','a\né€',false)"),
            ("long", "SELECT set_config('application_name','" + "x" * 100 + "',false)"),
            ("utf8_clip", "SELECT set_config('application_name','" + "x" * 62 + "é',false)"),
            ("expanded", "SELECT set_config('application_name','" + "é" * 32 + "',false)"),
            ("reset_expanded", "RESET application_name"),
            ("implicit_block_local", "SET LOCAL application_name='packet-local'; SELECT current_setting('application_name')"),
            ("implicit_block_set", "SET application_name='packet-session'; SELECT current_setting('application_name')"),
            ("implicit_block_reset", "RESET application_name; SHOW application_name"),
            ("timezone_coalesced", "SET TimeZone='Europe/Warsaw'; SET TimeZone='UTC'"),
            ("timezone_final", "SET TimeZone='Europe/Warsaw'; SHOW TimeZone"),
            ("timezone_reset", "RESET TimeZone; SHOW TimeZone"),
        )
        for label, sql in steps:
            wire.names.clear()
            wire.notices.clear()
            try:
                rows = wire.query(sql)
            except WireError as failure:
                raise AssertionError((label, sql, str(failure))) from failure
            observation = wire.observation()
            current = wire.query("SHOW application_name")
            result[label] = dict(rows=rows, current=current, **observation)
        wire.query("DROP TABLE IF EXISTS application_name_zero")
        wire.query("CREATE TABLE application_name_zero (id integer)")
        wire.query("INSERT INTO application_name_zero VALUES (0)")
        wire.names.clear()
        rows = wire.query("SET LOCAL application_name='packet-local-writer'; "
                          "INSERT INTO application_name_zero VALUES (4); "
                          "SELECT current_setting('application_name'),count(*) FROM application_name_zero")
        result["packet_write_success"] = dict(rows=rows, reports=list(wire.names),
                                              current=wire.query("SHOW application_name"))
        wire.query("DELETE FROM application_name_zero WHERE id=4")
        try:
            wire.query("SET SESSION LOCAL application_name='invalid'")
            raise AssertionError("conflicting SET scope modifiers succeeded")
        except WireError as failure:
            result["invalid_scope"] = failure.sqlstate
            assert failure.sqlstate == "42601"
        for label, sql in (
            ("packet_setting_error", "SET application_name='packet-error'; SELECT 1/id FROM application_name_zero"),
            ("packet_timezone_error", "SET TimeZone='Europe/Warsaw'; SELECT 1/id FROM application_name_zero"),
            ("packet_write_error", "INSERT INTO application_name_zero VALUES (4); SELECT 1/id FROM application_name_zero"),
        ):
            wire.names.clear()
            try:
                wire.query(sql)
                raise AssertionError("packet division by zero unexpectedly succeeded")
            except WireError as failure:
                assert failure.sqlstate == "22012"
            observation = wire.observation()
            result[label] = dict(**observation, current=wire.query("SHOW application_name"),
                                 rows=wire.query("SELECT count(*) FROM application_name_zero"))
        for label, begin in (("implicit_error", False), ("explicit_error", True)):
            if begin:
                wire.query("BEGIN")
            wire.names.clear()
            try:
                wire.query("SELECT set_config('application_name','error-side-effect',false),1/id FROM application_name_zero")
                raise AssertionError("division by zero unexpectedly succeeded")
            except WireError as failure:
                assert failure.sqlstate == "22012"
            reports = list(wire.names)
            if begin:
                wire.query("ROLLBACK")
            result[label] = dict(reports=reports, current=wire.query("SHOW application_name"))
        result["packet_cancel"] = packet_cancel(port, native)
        return json.loads(json.dumps(result))
    finally:
        wire.close()


def verify_reload(port, base, root, sbcl):
    connection = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                  dbname="postgres", application_name="reload-start", connect_timeout=3)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET application_name='session-reload'")
            cursor.execute("BEGIN; SET LOCAL application_name='local-reload'")
            backend_pid = connection.get_backend_pid()
            expression = (f'(defparameter *application-name-test-backend* (lookup-backend {backend_pid}))\n'
                          '(assert *application-name-test-backend*)\n'
                          '(assert (equal "local-reload" (backend-application-name *application-name-test-backend*)))\n'
                          '(reload)\n'
                          f'(assert (eq *application-name-test-backend* (lookup-backend {backend_pid})))\n'
                          '(assert (equal "local-reload" (backend-application-name *application-name-test-backend*)))\n')
            control(base, root, sbcl, expression)
            cursor.execute("SHOW application_name")
            assert cursor.fetchone() == ("local-reload",)
            assert connection.get_parameter_status("application_name") == "local-reload"
            cursor.execute("COMMIT")
            cursor.execute("SHOW application_name")
            assert cursor.fetchone() == ("session-reload",)
            assert connection.get_parameter_status("application_name") == "session-reload"
        print("application-name: live control reload preserves backend and LOCAL/session state PASS")
    finally:
        connection.close()


def verify_fatal_commit(port, base, root, sbcl):
    wire = ObservedConnection(port)
    installed = False
    try:
        control(base, root, sbcl, f'(load "{base}/t/application-name-native.lisp")')
        control(base, root, sbcl, f'(application-name-install-fatal {wire.backend_pid})')
        installed = True
        try:
            wire.query("INSERT INTO application_name_zero VALUES (99); SHOW application_name")
            raise AssertionError("post-commit FATAL was not delivered")
        except (EOFError, ConnectionError, WireError):
            assert wire.errors == ["57P01"], (wire.errors, wire.frames)
        deadline = time.monotonic() + 10
        while True:
            output = control(base, root, sbcl,
                             f'(if (lookup-backend {wire.backend_pid}) :waiting :gone)')
            if ":GONE" in output:
                break
            assert time.monotonic() < deadline
            time.sleep(0.025)
        control(base, root, sbcl, "(assert (application-name-verify-fatal))")
        observer = ObservedConnection(port)
        try:
            assert observer.query("SELECT count(*) FROM application_name_zero WHERE id=99") == [["1"]]
        finally:
            observer.close()
        print("application-name: real post-commit FATAL preserves row/CLOG and skips rollback PASS")
    finally:
        if installed:
            control(base, root, sbcl, "(application-name-restore-fatal)")
        wire.close()


if __name__ == "__main__":
    reference = observe(int(sys.argv[1]))
    native = (Path(sys.argv[3]), Path(sys.argv[4]), sys.argv[5])
    actual = observe(int(sys.argv[2]), native)
    for name in reference:
        assert actual[name] == reference[name], (name, actual[name], reference[name])
    print(json.dumps(actual, indent=2))
    print("application-name: C/native startup, SQL, transaction and wire contracts PASS")
    verify_fatal_commit(int(sys.argv[2]), *native)
    verify_reload(int(sys.argv[2]), *native)
