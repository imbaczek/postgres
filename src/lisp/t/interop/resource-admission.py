"""Exercise native resource admission using owned servers and the control REPL."""
import os
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import time

import psycopg2


BASE = Path(__file__).resolve().parents[2]
ROOT = Path(tempfile.mkdtemp(prefix="pg-resource-admission."))
ROOT.chmod(0o700)
PORT = int(os.environ.get("PORT", "6594"))
SERVERS = {}
LOGS = []


def wait_for(predicate, description, seconds=45):
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out: {description}")
        time.sleep(0.05)


def connect(name):
    return psycopg2.connect(host="127.0.0.1", port=PORT + (name == "target"),
                            user="postgres", dbname="postgres", connect_timeout=2)


def sql(name, query):
    connection = connect(name)
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()[0] if cursor.description else None
    finally:
        connection.close()


def startup_error_sqlstate(name):
    def read_exact(stream, size):
        data = bytearray()
        while len(data) < size:
            part = stream.recv(size - len(data))
            assert part, "startup response ended before ErrorResponse"
            data.extend(part)
        return bytes(data)

    with socket.create_connection(("127.0.0.1", PORT + (name == "target")), timeout=5) as stream:
        body = struct.pack("!I", 196608) + b"user\0postgres\0database\0postgres\0\0"
        stream.sendall(struct.pack("!I", len(body) + 4) + body)
        while True:
            kind = read_exact(stream, 1)
            length = struct.unpack("!I", read_exact(stream, 4))[0]
            assert 4 <= length <= 1048576
            payload = read_exact(stream, length - 4)
            assert kind != b"Z", "startup unexpectedly succeeded"
            if kind == b"E":
                fields = {part[:1]: part[1:] for part in payload.split(b"\0") if part}
                return fields[b"C"].decode("ascii")


def start(name, connections=100, ready=True, hot_standby=True, workers=None):
    socket = ROOT / f"{name}.sock"
    socket.unlink(missing_ok=True)
    log = open(ROOT / f"{name}-{connections}.log", "ab")
    LOGS.append(log)
    arguments = [str(BASE / "pg-lisp.sh"), "-p", str(PORT + (name == "target")),
         "-D", str(ROOT / name), "--control-socket", str(socket),
         "-c", f"max_connections={connections}",
         "-c", f"hot_standby={'on' if hot_standby else 'off'}"]
    if workers is not None:
        arguments += ["-c", f"max_worker_processes={workers}"]
    process = subprocess.Popen(arguments, stdout=log, stderr=log)
    SERVERS[name] = process
    if not ready:
        assert process.wait(timeout=45) == 1
        return

    def listening():
        assert process.poll() is None, f"{name} exited; inspect {ROOT}"
        if not hot_standby:
            if not socket.exists():
                return False
            control(name, "(assert (recovery-in-progress-p))")
            return True
        try:
            result = sql(name, "SELECT 1") == 1
        except psycopg2.OperationalError:
            return False
        pid = int((ROOT / name / "postmaster.pid").read_text().splitlines()[0])
        assert pid == process.pid, "connected to an unrelated server"
        return result

    wait_for(listening, f"{name} startup", 90)


def control(name, form):
    process = SERVERS[name]
    assert process.poll() is None
    request = f"(assert (= (sb-posix:getpid) {process.pid}))\n{form}\n(quit)\n"
    result = subprocess.run(
        [os.environ.get("SBCL", "sbcl"), "--script", str(BASE / "t/control-client.lisp"),
         "unix", str(ROOT / f"{name}.sock")], input=request, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30, check=True)
    assert ";; error:" not in result.stdout and ";; read error:" not in result.stdout, result.stdout
    return result.stdout


def exercise():
    resource_field = os.environ.get("RESOURCE_FIELD", "connections")
    assert resource_field in ("connections", "workers"), resource_field
    start("source")
    sql("source", "CREATE TABLE data (id integer); INSERT INTO data VALUES (1)")
    (ROOT / "target").mkdir(mode=0o700)
    control("source", f'(physical-base-backup "{ROOT}/target/" "resource_admission")')
    start("target")
    control("target", "(physical-recovery-enable-streaming)")
    control("target", f'(physical-follow-start "127.0.0.1" {PORT} "resource_admission")')
    reader = connect("target")
    try:
        with reader.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM data")
            assert cursor.fetchone() == (1,)
        promotion_backlog = os.environ.get("RESOURCE_PROMOTION_BACKLOG") == "1"
        if promotion_backlog:
            sql("target", "SELECT pg_wal_replay_pause()")
        # Inject through the real reporter so the pause does not depend on
        # restarting the source with a larger local capacity first.
        required = "100 16 10 0 128" if resource_field == "workers" else "200 8 10 0 64"
        control("source", f"(wal-report-resource-parameters (make-resource-parameters {required}))")
        sql("source", "INSERT INTO data VALUES (2)")
        end = sql("source", "SELECT pg_current_wal_flush_lsn()")
        wait_for(lambda: sql("target", f"SELECT pg_last_wal_receive_lsn() >= '{end}'::pg_lsn"),
                 "receipt beyond resource pause")
        if promotion_backlog:
            assert sql("target", "SELECT pg_promote(false)")
            wait_for(lambda: ":RESTORED" in control(
                "target", "(if (and *physical-resource-pause* (not *physical-promotion-stopping-p*)) :restored :pending)"
            ).upper(), "promotion backlog refusal and worker restoration")
        else:
            wait_for(lambda: sql("target", "SELECT pg_is_wal_replay_paused()"), "resource pause")
        assert not sql("target", f"SELECT pg_last_wal_replay_lsn() >= '{end}'::pg_lsn")
        with reader.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM data")
            assert cursor.fetchone() == (1,)
        assert sql("target", "SELECT count(*) FROM data") == 1
        try:
            sql("target", "SELECT pg_promote(false)")
        except psycopg2.Error as error:
            assert error.pgcode == "22023", error
        else:
            raise AssertionError("resource-paused standby accepted promotion")
        assert not (ROOT / "target/pg_wal/00000002.history").exists()
        control("target", "(assert (and *physical-resource-pause* (not *physical-recovery-failed-p*)))")
        sql("target", "SELECT pg_wal_replay_resume()")
        assert SERVERS["target"].wait(timeout=45) == 0
    finally:
        reader.close()
    start("target", ready=False)
    assert "insufficient parameter settings" in (ROOT / "target-100.log").read_text()
    if resource_field == "workers":
        start("target", workers=16)
        assert sql("target", "SHOW max_worker_processes") == "16"
    else:
        start("target", connections=200)
    assert sql("target", "SELECT pg_is_in_recovery()")
    assert sql("target", "SELECT count(*) FROM data") == 2
    print("resource-admission: retained reader, durable pause, receipt, promotion refusal, "
          "resume shutdown and adequate restart PASS")
    if promotion_backlog:
        print("resource-admission: promotion discovers resource shortfall in backlog and restores standby PASS")
    if resource_field == "workers":
        print("resource-admission: worker requirement pauses replay; adequate worker capacity restart clears it PASS")


def legacy_unknown():
    SERVERS["target"].kill()
    SERVERS["target"].wait(timeout=15)
    (ROOT / "target").rename(ROOT / "adequate-target")
    (ROOT / "target").mkdir(mode=0o700)
    # Produce an old-style checkpoint/control pair through the real backup
    # writer. The temporary binding removes only the optional report group.
    control("source", f'''(progn
      (defparameter *resource-legacy-status* :running)
      (sb-thread:make-thread
       (lambda ()
         (handler-case
             (let ((*wal-control-metadata* (copy-list *wal-control-metadata*)))
               (dolist (key '(:resource-parameters :resource-lsn :resource-timeline))
                 (remf *wal-control-metadata* key))
               (physical-base-backup "{ROOT}/target/" "unknown_resources")
               (setf *resource-legacy-status* :done))
           (error (condition)
             (setf *resource-legacy-status* (list :error (princ-to-string condition))))))))''')

    def copied():
        status = control("source", "*resource-legacy-status*").upper()
        assert ":ERROR" not in status, status
        return ":DONE" in status

    wait_for(copied, "unknown resource backup")
    start("target", ready=False)
    assert "requires known source resource parameters" in (ROOT / "target-100.log").read_text()
    start("target", hot_standby=False)
    control("target", "(assert (and (not *enable-hot-standby*) (not (getf *physical-recovery-state* :source-resource-fact))))")
    try:
        sql("target", "SELECT 1")
    except psycopg2.Error as error:
        assert "Hot standby mode is disabled" in str(error), error
        assert startup_error_sqlstate("target") == "57P03"
    else:
        raise AssertionError("hot_standby=off admitted SQL")
    print("resource-admission: unknown legacy facts reject hot standby and permit OFF recovery PASS")


def lock_setting_reload():
    assert sql("source", "SHOW max_locks_per_transaction") == "128"
    (ROOT / "source/postgresql.conf").write_text("max_locks_per_transaction=149\n")
    control("source", "(reload)")
    assert sql("source", "SHOW max_locks_per_transaction") == "128"
    assert sql("source", "SELECT pending_restart FROM pg_settings WHERE name='max_locks_per_transaction'")
    control("source", "(assert (and (= 128 (resource-parameters-locks *installed-resource-parameters*)) (= (* 128 136) *relation-lock-object-limit*) (= (* 2 128 136) *relation-lock-owner-limit*)))")
    SERVERS["source"].send_signal(2)
    assert SERVERS["source"].wait(timeout=45) == 0
    start("source")
    assert sql("source", "SHOW max_locks_per_transaction") == "149"
    assert not sql("source", "SELECT pending_restart FROM pg_settings WHERE name='max_locks_per_transaction'")
    control("source", "(assert (and (= 149 (resource-parameters-locks *installed-resource-parameters*)) (= (* 149 136) *relation-lock-object-limit*) (= (* 2 149 136) *relation-lock-owner-limit*) (= 149 (fifth (getf *wal-control-metadata* :resource-parameters)))))")
    control("source", "(let ((end *wal-end-lsn*)) (wal-report-installed-resource-capacity) (assert (= end *wal-end-lsn*)))")
    print("resource-admission: lock GUC reload retains installed pool; restart sizes and reports new pool PASS")


try:
    exercise()
    legacy_unknown()
    lock_setting_reload()
finally:
    for process in SERVERS.values():
        if process.poll() is None:
            process.kill()
            process.wait(timeout=15)
    for log in LOGS:
        log.close()
    print(f"resource-admission: artifacts={ROOT}")
