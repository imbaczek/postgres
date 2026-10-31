"""Exercise native HBA admission and reloads through owned SQL/control sockets."""
import os
from pathlib import Path
import re
import signal
import socket
import struct
import subprocess
import tempfile
import time

import psycopg2


BASE = Path(__file__).resolve().parents[2]
ROOT = Path(tempfile.mkdtemp(prefix="pg-hba-policy."))
ROOT.chmod(0o700)
DATA = ROOT / "data"
CONFIG = ROOT / "postgresql.conf"
HBA = ROOT / "policy.hba"
IDENT = ROOT / "policy.ident"
SOCKET = ROOT / "control.sock"
TAG = ROOT.name
SERVER = HELD = SPARE = None
with socket.socket() as reservation:
    reservation.bind(("127.0.0.1", int(os.environ.get("PORT", "0"))))
    PORT = reservation.getsockname()[1]

INITIAL = ("host all blocked all reject\n"
           "host all postgres 127.0.0.1/32 trust\n"
           "host sameuser alice 127.0.0.1/32 trust\n"
           "host replication repl 127.0.0.1/32 trust\n")


def configuration(timeout=30, hba=HBA):
    CONFIG.write_text(f"cluster_name='{TAG}'\nhba_file='{hba}'\nident_file='{IDENT}'\n"
                      f"max_connections=2\nsuperuser_reserved_connections=0\n"
                      f"checkpoint_timeout={timeout}\n")


def connect():
    return psycopg2.connect(host="127.0.0.1", port=PORT, user="postgres",
                            dbname="postgres", connect_timeout=2)


def query(sql, parameters=None):
    with HELD.cursor() as cursor:
        cursor.execute(sql, parameters)
        return cursor.fetchall()


def start(log):
    global SERVER, HELD
    assert SERVER is None or SERVER.poll() is not None
    SOCKET.unlink(missing_ok=True)
    SERVER = subprocess.Popen(
        [str(BASE / "pg-lisp.sh"), "-D", str(DATA), "-p", str(PORT),
         "-c", f"config_file={CONFIG}", "--control-socket", str(SOCKET)],
        stdout=log, stderr=log)
    deadline = time.monotonic() + 90
    while True:
        assert SERVER.poll() is None, f"server exited; inspect {ROOT}"
        try:
            HELD = connect()
            HELD.autocommit = True
            assert query("SHOW cluster_name") == [(TAG,)]
            assert int((DATA / "postmaster.pid").read_text().splitlines()[0]) == SERVER.pid
            return
        except psycopg2.OperationalError:
            assert time.monotonic() < deadline, f"startup timed out; inspect {ROOT}"
            time.sleep(0.05)


def stop():
    global HELD, SPARE
    for connection in (SPARE, HELD):
        if connection is not None:
            connection.close()
    HELD = SPARE = None
    SERVER.send_signal(signal.SIGINT)
    assert SERVER.wait(timeout=45) == 0


def control(form, server=None, control_socket=None):
    server = SERVER if server is None else server
    control_socket = SOCKET if control_socket is None else control_socket
    assert server.poll() is None
    result = subprocess.run(
        [os.environ.get("SBCL", "sbcl"), "--script", str(BASE / "t/control-client.lisp"),
         "unix", str(control_socket)],
        input=f"(assert (= (sb-posix:getpid) {server.pid}))\n{form}\n(quit)\n",
        text=True, capture_output=True, timeout=45, check=True)
    assert ";; error:" not in result.stdout, result.stdout
    return result.stdout


def wait_sessions(count):
    deadline = time.monotonic() + 10
    while True:
        result = control('(format nil "SESSIONS=~d" (hash-table-count *backend-admission-classes*))')
        observed = int(re.search(r"SESSIONS=(\d+)", result).group(1))
        if observed == count:
            return
        assert time.monotonic() < deadline, (count, observed, ROOT)
        time.sleep(0.05)


def exact(stream, size):
    data = bytearray()
    while len(data) < size:
        chunk = stream.recv(size - len(data))
        assert chunk, "unexpected startup EOF"
        data.extend(chunk)
    return bytes(data)


def startup(params, port=None):
    body = struct.pack("!I", 196608)
    for key, value in params:
        body += key.encode() + b"\0" + value.encode() + b"\0"
    body += b"\0"
    authenticated = False
    with socket.create_connection(("127.0.0.1", PORT if port is None else port), timeout=3) as stream:
        stream.sendall(struct.pack("!I", len(body) + 4) + body)
        while True:
            kind = exact(stream, 1)
            size = struct.unpack("!I", exact(stream, 4))[0]
            assert 4 <= size <= 1048576, size
            payload = exact(stream, size - 4)
            if kind == b"R":
                assert payload == b"\0\0\0\0", payload
                authenticated = True
            elif kind == b"E":
                fields = {field[:1].decode(): field[1:].decode()
                          for field in payload.split(b"\0") if field}
                return authenticated, fields
            elif kind == b"Z":
                assert authenticated
                return authenticated, None


def denied(params, code="28000", port=None):
    authenticated, error = startup(params, port)
    assert error and not authenticated and error["C"] == code and error["S"] == "FATAL", error


def exercise(log):
    global SPARE
    HBA.write_text(INITIAL)
    IDENT.write_text("# no identity mappings\n")
    configuration()
    start(log)
    SPARE = connect()
    SPARE.autocommit = True
    wait_sessions(2)
    denied([("user", "blocked"), ("database", "never_created")])
    assert query("SELECT datname FROM pg_database WHERE datname='never_created'") == []
    SPARE.close()
    SPARE = None
    wait_sessions(1)
    for params in ([('user', 'alice')], [('user', 'alice'), ('database', '')],
                   [('user', 'repl'), ('replication', 'true')],
                   [('user', 'postgres'), ('database', 'postgres'), ('replication', 'database')]):
        assert startup(params) == (True, None), params
        wait_sessions(1)
    denied([("user", "postgres"), ("replication", "true")])
    denied([("user", "unknown"), ("database", "never_created")])
    denied([])
    denied([("user", "postgres"), ("user", "")])
    HBA.write_text("host all all all reject\n")
    configuration(90)
    assert "reload complete" in control("(pg::reload)")
    assert query("SELECT 1") == [(1,)]
    denied([("user", "postgres"), ("database", "postgres")])
    HBA.write_text("host all all all md5\n")
    configuration(120)
    assert "reload failed" in control("(pg::reload)")
    assert query("SELECT setting FROM pg_settings WHERE name='checkpoint_timeout'") == [("90",)]
    denied([("user", "postgres"), ("database", "postgres")])
    HBA.write_text(INITIAL)
    configuration(120, ROOT / "missing.hba")
    assert "reload complete" in control("(pg::reload)")
    assert query("SELECT setting,pending_restart FROM pg_settings WHERE name='hba_file'") == [(str(HBA), True)]
    assert startup([("user", "postgres"), ("database", "postgres")]) == (True, None)
    wait_sessions(1)
    configuration()
    assert "reload complete" in control("(pg::reload)")
    stop()
    IDENT.unlink()
    start(log)
    denied([("user", "blocked"), ("database", "never_created")])
    stop()


def cleanup():
    for connection in (SPARE, HELD):
        if connection is not None:
            connection.close()
    if SERVER is not None and SERVER.poll() is None:
        SERVER.send_signal(signal.SIGINT)
        try:
            SERVER.wait(timeout=45)
        except subprocess.TimeoutExpired:
            SERVER.kill()
            SERVER.wait(timeout=10)


if __name__ == "__main__":
    try:
        with (ROOT / "server.log").open("w") as log:
            exercise(log)
        print(f"hba-policy: PASS ({ROOT})")
    finally:
        cleanup()
