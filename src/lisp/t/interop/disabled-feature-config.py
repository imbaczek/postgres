"""Check disabled feature settings before storage startup and in an owned server."""
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time

import psycopg2


BASE = Path(__file__).resolve().parents[2]
ROOT = Path(tempfile.mkdtemp(prefix="pg-disabled-feature-config."))
ROOT.chmod(0o700)
PORT = int(os.environ.get("PORT", "6614"))
SETTINGS = {
    "max_prepared_transactions": ("0", "1"),
    "track_commit_timestamp": ("off", "on"),
    "recovery_target": ("", "immediate"),
    "recovery_target_lsn": ("", "0/10"),
    "recovery_target_name": ("", "stop_here"),
    "recovery_target_time": ("", "2026-10-30 00:00:00+00"),
    "recovery_target_xid": ("", "42"),
}
DATA = ROOT / "data"
CONFIG = ROOT / "postgresql.conf"
SOCKET = ROOT / "control.sock"
SERVER = None


def sql(query):
    connection = psycopg2.connect(host="127.0.0.1", port=PORT, user="postgres",
                                  dbname="postgres", connect_timeout=2)
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall() if cursor.description else None
    finally:
        connection.close()


def control(form):
    assert SERVER.poll() is None
    request = f"(assert (= (sb-posix:getpid) {SERVER.pid}))\n{form}\n(quit)\n"
    result = subprocess.run(
        [os.environ.get("SBCL", "sbcl"), "--script", str(BASE / "t/control-client.lisp"),
         "unix", str(SOCKET)], input=request, text=True, capture_output=True,
        timeout=45, check=True)
    assert ";; error:" not in result.stdout, result.stdout
    return result.stdout


def write_configuration(enabled=None):
    lines = ["include='included.conf'", "checkpoint_timeout=90"]
    lines += [f"{name}='{on if name == enabled else off}'"
              for name, (off, on) in SETTINGS.items()]
    CONFIG.write_text("\n".join(lines) + "\n")


def start(log):
    global SERVER
    SOCKET.unlink(missing_ok=True)
    SERVER = subprocess.Popen(
        [str(BASE / "pg-lisp.sh"), "-D", str(DATA), "-p", str(PORT),
         "-c", f"config_file={CONFIG}", "--control-socket", str(SOCKET)],
        stdout=log, stderr=log)
    deadline = time.monotonic() + 90
    while True:
        assert SERVER.poll() is None, f"server exited; inspect {ROOT}"
        try:
            if sql("SELECT 1") == [(1,)]:
                pid = int((DATA / "postmaster.pid").read_text().splitlines()[0])
                assert pid == SERVER.pid, "connected to an unrelated server"
                return
        except psycopg2.OperationalError:
            pass
        assert time.monotonic() < deadline, f"startup timed out; inspect {ROOT}"
        time.sleep(0.05)


def check_settings():
    for name, (disabled, _) in SETTINGS.items():
        assert sql(f"SHOW {name}") == [(disabled,)]
        row = sql("SELECT setting, context, vartype, pending_restart FROM pg_settings "
                  f"WHERE name='{name}'")
        kind = "integer" if name == "max_prepared_transactions" else (
            "bool" if name == "track_commit_timestamp" else "string")
        assert row == [(disabled, "postmaster", kind, False)], (name, row)
    control("(assert (and (zerop (resource-parameters-prepared *installed-resource-parameters*)) "
            "(zerop (fourth (getf *wal-control-metadata* :resource-parameters)))))")


try:
    for name, (_, enabled) in SETTINGS.items():
        missing = ROOT / f"refused-{name}"
        result = subprocess.run(
            [str(BASE / "pg-lisp.sh"), "-D", str(missing), "-c", f"{name}={enabled}"],
            text=True, capture_output=True, timeout=45)
        assert result.returncode != 0, name
        assert "requires unimplemented" in result.stderr, (name, result.stderr)
        assert not missing.exists(), f"unsupported {name} created storage"
    print("disabled-feature-config: enabling values fail before creating PGDATA PASS", flush=True)

    (ROOT / "included.conf").write_text("\n".join(
        f"{name}='{enabled}'" for name, (_, enabled) in SETTINGS.items()) + "\n")
    write_configuration()
    with open(ROOT / "server.log", "ab") as log:
        start(log)
        check_settings()
        sql("CREATE TABLE witness (id integer); INSERT INTO witness VALUES (1)")
        control("(defparameter *disabled-feature-original-capacity* *installed-resource-parameters*)")
        for name in SETTINGS:
            write_configuration(enabled=name)
            assert "reload failed" in control("(reload)"), name
            check_settings()
            control("(assert (eq *disabled-feature-original-capacity* *installed-resource-parameters*))")
            assert sql("SELECT id FROM witness") == [(1,)]
        write_configuration()
        assert "reload complete" in control("(reload)")
        SERVER.send_signal(signal.SIGINT)
        assert SERVER.wait(timeout=45) == 0
        start(log)
        check_settings()
        assert sql("SELECT id FROM witness") == [(1,)]
        SERVER.send_signal(signal.SIGINT)
        assert SERVER.wait(timeout=45) == 0
    print("disabled-feature-config: include overrides, SQL metadata, rejected reloads and restart PASS")
finally:
    if SERVER is not None and SERVER.poll() is None:
        SERVER.kill()
        SERVER.wait(timeout=15)
    print(f"disabled-feature-config: artifacts={ROOT}")
