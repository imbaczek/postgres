"""Exercise WAL capture settings and read-side hint safety in an owned server."""
import hashlib
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time

import psycopg2
from psycopg2.extras import LogicalReplicationConnection


BASE = Path(__file__).resolve().parents[2]
ROOT = Path(tempfile.mkdtemp(prefix="pg-wal-settings."))
ROOT.chmod(0o700)
PORT = int(os.environ.get("PORT", "6624"))
SERVER = None
DATA = CONFIG = SOCKET = None


def sql(query, parameters=None):
    connection = psycopg2.connect(host="127.0.0.1", port=PORT, user="postgres",
                                  dbname="postgres", connect_timeout=2)
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            return cursor.fetchall() if cursor.description else None
    finally:
        connection.close()


def control(form):
    assert SERVER.poll() is None
    result = subprocess.run(
        [os.environ.get("SBCL", "sbcl"), "--script", str(BASE / "t/control-client.lisp"),
         "unix", str(SOCKET)],
        input=f"(assert (= (sb-posix:getpid) {SERVER.pid}))\n{form}\n(quit)\n",
        text=True, capture_output=True, timeout=45, check=True)
    assert ";; error:" not in result.stdout, result.stdout
    return result.stdout


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
                assert int((DATA / "postmaster.pid").read_text().splitlines()[0]) == SERVER.pid
                assert sql("SELECT enumvals FROM pg_settings WHERE name='wal_level'") == [
                    (["minimal", "replica", "logical"],)]
                assert sql("SELECT enumvals FROM pg_settings WHERE name IN "
                           "('wal_log_hints','application_name','statement_timeout') ORDER BY name") == [(None,)] * 3
                return
        except psycopg2.OperationalError:
            pass
        assert time.monotonic() < deadline, f"startup timed out; inspect {ROOT}"
        time.sleep(0.05)


def stop():
    SERVER.send_signal(signal.SIGINT)
    assert SERVER.wait(timeout=45) == 0


def write_configuration(level, hints):
    CONFIG.write_text(f"include='included.conf'\nwal_level={level}\nwal_log_hints={hints}\n")


def optional_records(query, expected):
    control("(defparameter *wal-setting-test-start* "
            "(sb-thread:with-mutex (*wal-lock*) *wal-durable-lsn*))")
    sql(query)
    result = control("""(sb-thread:with-mutex (*wal-lock*)
      (let ((count 0))
        (wal-scan (lambda (record)
                    (when (member (wal-record-type record)
                                  (list +wal-record-logical-changes+ +wal-record-logical-catalog-event+))
                      (incf count)))
                  *wal-setting-test-start* :end-lsn *wal-durable-lsn*)
        (format nil "OPTIONAL=~d" count)))""")
    count = int(re.search(r"OPTIONAL=(\d+)", result).group(1))
    assert (count > 0) == expected, (query, count, expected)


def replication(query):
    connection = psycopg2.connect(
        host="127.0.0.1", port=PORT, user="postgres", dbname="postgres",
        connection_factory=LogicalReplicationConnection, connect_timeout=2)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
    finally:
        connection.close()


def page_hashes():
    return {str(path.relative_to(DATA)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (DATA / "base").rglob("*") if path.is_file()}


try:
    missing = ROOT / "refused-minimal"
    result = subprocess.run(
        [str(BASE / "pg-lisp.sh"), "-D", str(missing), "-c", "wal_level=minimal",
         "-c", "max_wal_senders=0"], text=True, capture_output=True, timeout=45)
    assert result.returncode != 0
    assert "unimplemented minimal WAL generation" in result.stderr, result.stderr
    assert not missing.exists(), "minimal WAL refusal created storage"
    (ROOT / "included.conf").write_text("wal_level=minimal\n")
    for hints in ("off", "on"):
        DATA = ROOT / f"data-{hints}"
        CONFIG = ROOT / f"postgresql-{hints}.conf"
        SOCKET = ROOT / f"control-{hints}.sock"
        write_configuration("replica", hints)
        with open(ROOT / f"server-{hints}.log", "ab") as log:
            start(log)
            sql("CREATE TABLE witness (id integer PRIMARY KEY, payload text)")
            for level in ("replica", "logical"):
                assert sql("SHOW wal_level") == [(level,)]
                control(f"(assert (eq *wal-level* :{level}))")
                for query in ("INSERT INTO witness VALUES (1,'first')",
                              "UPDATE witness SET payload='changed' WHERE id=1",
                              "DELETE FROM witness WHERE id=1",
                              "INSERT INTO witness VALUES (2,'second')",
                              "TRUNCATE witness"):
                    optional_records(query, level == "logical")
                sql("INSERT INTO witness VALUES (42,%s)", ("toast-value" * 2000,))
                before = page_hashes()
                for _ in range(3):
                    assert sql("SELECT id, payload FROM witness") == [(42, "toast-value" * 2000)]
                assert before == page_hashes(), (level, hints, "scan changed persistent pages")
                sql("DELETE FROM witness")
                if level == "replica":
                    write_configuration("logical", "on" if hints == "off" else "off")
                    assert "reload complete" in control("(reload)")
                    assert sql("SHOW wal_level") == [("replica",)]
                    assert sql("SHOW wal_log_hints") == [(hints,)]
                    assert sql("SELECT pending_restart FROM pg_settings WHERE name='wal_level'") == [(True,)]
                    assert sql("SELECT pending_restart FROM pg_settings WHERE name='wal_log_hints'") == [(True,)]
                    control("(assert (eq *wal-level* :replica))")
                    optional_records("INSERT INTO witness VALUES (3,'pending')", False)
                    sql("DELETE FROM witness")
                    stop()
                    write_configuration("logical", hints)
                    start(log)
            replication("CREATE_REPLICATION_SLOT wal_setting_slot LOGICAL pgoutput NOEXPORT_SNAPSHOT")
            stop()
            write_configuration("replica", hints)
            start(log)
            assert sql("SELECT slot_name FROM pg_replication_slots WHERE slot_name='wal_setting_slot'") == [("wal_setting_slot",)]
            optional_records("INSERT INTO witness VALUES (7,'slot-driven')", True)
            replication("DROP_REPLICATION_SLOT wal_setting_slot")
            optional_records("INSERT INTO witness VALUES (8,'no-slot')", False)
            stop()
        print(f"wal-settings: hints={hints}, forced and slot-driven WAL, reload, slot restart and page hashes PASS", flush=True)
finally:
    if SERVER is not None and SERVER.poll() is None:
        SERVER.kill()
        SERVER.wait(timeout=15)
    print(f"wal-settings: artifacts={ROOT}")
