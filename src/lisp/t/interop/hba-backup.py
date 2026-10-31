"""Keep active HBA policy through backup, mutable standby config and promotion."""
import importlib.util
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time

import psycopg2


spec = importlib.util.spec_from_file_location("hba_policy", Path(__file__).with_name("hba-policy.py"))
hba = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hba)
ROOT = hba.ROOT
BASE = ROOT / "backup"
STRIPPED = ROOT / "stripped"
CHILD = ROOT / "child-backup"
CONTROL = ROOT / "standby.sock"
PROCESSES = []
LOCAL_POLICY = "host all postgres 127.0.0.1/32 trust\nhost all all all reject\n"
with socket.socket() as reservation:
    reservation.bind(("127.0.0.1", 0))
    PORT = reservation.getsockname()[1]


def start_standby(log):
    CONTROL.unlink(missing_ok=True)
    process = subprocess.Popen(
        [str(hba.BASE / "pg-lisp.sh"), "-D", str(BASE), "-p", str(PORT),
         "--control-socket", str(CONTROL)], stdout=log, stderr=log)
    PROCESSES.append(process)
    deadline = time.monotonic() + 90
    while True:
        assert process.poll() is None, f"standby exited; inspect {ROOT}"
        pid = BASE / "postmaster.pid"
        if pid.exists() and CONTROL.exists():
            lines = pid.read_text().splitlines()
            if len(lines) >= 8 and lines[7].strip() == "ready":
                assert int(lines[0]) == process.pid
                hba.control("(sb-posix:getpid)", process, CONTROL)
                return process
        assert time.monotonic() < deadline, f"standby startup timed out; inspect {ROOT}"
        time.sleep(0.05)


def stop_standby(process):
    process.send_signal(signal.SIGINT)
    assert process.wait(timeout=45) == 0


def assert_policy(directory, expected, backup=True):
    assert (directory / "pg_hba.conf").read_text() == expected
    assert (directory / "authentication.initialized").is_file()
    if backup:
        assert (directory / "base").is_dir()
        assert ":AUTH-POLICY-REQUIRED 1" in (directory / "physical-backup.manifest").read_text().upper()


def stripped_backup_refuses_startup(log):
    shutil.copytree(BASE, STRIPPED)
    for name in ("pg_hba.conf", "pg_ident.conf", "authentication.initialized"):
        (STRIPPED / name).unlink()
    interrupted = ROOT / "interrupted"
    interrupted.mkdir(mode=0o700)
    shutil.copyfile(BASE / "recovery.required", interrupted / "recovery.required")
    for directory in (STRIPPED, interrupted):
        result = subprocess.run(
            [str(hba.BASE / "pg-lisp.sh"), "-D", str(directory), "-p", str(PORT)],
            stdout=log, stderr=log, timeout=45)
        assert result.returncode != 0
        for name in ("pg_hba.conf", "pg_ident.conf", "authentication.initialized", "postmaster.pid"):
            assert not (directory / name).exists(), name


def source_backup(log):
    hba.HBA.write_text(LOCAL_POLICY)
    hba.IDENT.write_text("# active identity mapping snapshot\n")
    hba.configuration()
    hba.start(log)
    hba.HBA.write_text("host all all all reject\n")
    assert "reload complete" in hba.control("(pg::reload)")
    hba.HBA.write_text("host all all all md5\n")
    BASE.mkdir(mode=0o700)
    hba.control(f'(pg::physical-base-backup "{BASE}/" "auth_copy")')
    assert_policy(BASE, "host all all all reject\n")
    assert (BASE / "pg_ident.conf").read_text() == "# active identity mapping snapshot\n"
    hba.stop()


def standby_and_promotion(log):
    assert not list((BASE / "base").iterdir())
    (BASE / "base").rmdir()  # Older empty native backups omitted this directory.
    process = start_standby(log)
    assert (BASE / "base").is_dir()
    hba.denied([("user", "postgres"), ("database", "postgres")], port=PORT)
    (BASE / "pg_hba.conf").write_text(LOCAL_POLICY)
    assert "reload complete" in hba.control("(pg::reload)", process, CONTROL)
    hba.control("(pg::physical-recovery-enable-streaming)", process, CONTROL)
    if os.environ.get("DEBUG_PROMOTION"):
        hba.control(f'''(let ((original (symbol-function 'physical-promotion-try-online)))
          (setf (symbol-function 'physical-promotion-try-online)
            (lambda ()
              (unwind-protect
                   (handler-bind
                       ((error (lambda (condition)
                          (ignore-errors
                            (with-open-file (out "{ROOT}/promotion-error.log"
                                                 :direction :output :if-exists :append
                                                 :if-does-not-exist :create)
                              (format out "PID=~d PGDATA=~a~%~a~%" (sb-posix:getpid) *pgdata* condition)
                              (sb-debug:print-backtrace :stream out :count 45))))))
                     (funcall original))
                (setf (symbol-function 'physical-promotion-try-online) original)))))''', process, CONTROL)
    connection = psycopg2.connect(host="127.0.0.1", port=PORT, user="postgres",
                                  dbname="postgres", connect_timeout=2)
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_is_in_recovery()")
            assert cursor.fetchall() == [(True,)]
            cursor.execute("SELECT pg_promote(true, 10)")
            assert cursor.fetchall() == [(True,)]
            cursor.execute("SELECT pg_is_in_recovery()")
            assert cursor.fetchall() == [(False,)]
        hba.denied([("user", "other"), ("database", "postgres")], port=PORT)
    finally:
        connection.close()
    assert_policy(BASE, LOCAL_POLICY, backup=False)
    stop_standby(process)
    process = start_standby(log)
    assert hba.startup([("user", "postgres"), ("database", "postgres")], PORT) == (True, None)
    hba.denied([("user", "other"), ("database", "postgres")], port=PORT)
    assert_policy(BASE, LOCAL_POLICY, backup=False)
    connection = psycopg2.connect(host="127.0.0.1", port=PORT, user="postgres",
                                  dbname="postgres", connect_timeout=2)
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE after_empty_promotion (id integer)")
            cursor.execute("INSERT INTO after_empty_promotion VALUES (1)")
            cursor.execute("SELECT id FROM after_empty_promotion")
            assert cursor.fetchall() == [(1,)]
    finally:
        connection.close()
    CHILD.mkdir(mode=0o700)
    hba.control(f'(pg::physical-base-backup "{CHILD}/" "auth_child_copy")', process, CONTROL)
    assert_policy(CHILD, LOCAL_POLICY)
    stop_standby(process)


def missing_listed_fork_refuses_startup(log):
    damaged = ROOT / "missing-fork"
    shutil.copytree(CHILD, damaged)
    fork = next((damaged / "base").iterdir())
    fork.unlink()
    catalog = (damaged / "global/pg_catalog.dat").read_bytes()
    result = subprocess.run(
        [str(hba.BASE / "pg-lisp.sh"), "-D", str(damaged), "-p", str(PORT)],
        stdout=log, stderr=log, timeout=45)
    assert result.returncode != 0
    assert not fork.exists()
    assert (damaged / "global/pg_catalog.dat").read_bytes() == catalog


try:
    print(f"hba-backup artifact: {ROOT}", flush=True)
    with (ROOT / "backup.log").open("w") as log:
        source_backup(log)
        stripped_backup_refuses_startup(log)
        standby_and_promotion(log)
        missing_listed_fork_refuses_startup(log)
    print(f"hba-backup: PASS ({ROOT})")
finally:
    hba.cleanup()
    for process in PROCESSES:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=45)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
