"""Exercise pinned Patroni's initial native primary bootstrap without a DCS."""

import argparse
import ctypes
from contextlib import closing
import importlib.metadata
import os
from pathlib import Path
import runpy
import shlex
import shutil
import signal
import socket
import subprocess
import tempfile

import psycopg2
from psycopg2.extras import PhysicalReplicationConnection
from patroni.postgresql import Postgresql
from patroni.postgresql.mpp import Null
from patroni.async_executor import CriticalTask


FOLLOW = runpy.run_path(str(Path(__file__).with_name("patroni-follow.py")))
BIN = FOLLOW["BASE"] / "bin"


def check_rejected_targets(root):
    occupied = root / "occupied"
    occupied.mkdir(mode=0o700)
    marker = occupied / ".hidden"
    marker.write_bytes(b"keep")
    for target in (occupied, root / "linked"):
        if target != occupied:
            target.symlink_to(occupied, target_is_directory=True)
        result = subprocess.run([BIN / "initdb", "-D", target], capture_output=True, timeout=20)
        assert result.returncode == 1 and marker.read_bytes() == b"keep", result
        assert list(occupied.iterdir()) == [marker]
    unsupported = root / "unsupported"
    result = subprocess.run([BIN / "initdb", "--auth-host=scram-sha-256", unsupported],
                            capture_output=True, timeout=20)
    assert result.returncode == 2 and not unsupported.exists(), result
    failed = root / "failed"
    result = subprocess.run([BIN / "initdb", failed], capture_output=True, timeout=20,
                            env={**os.environ, "SBCL": str(root / "missing-sbcl")})
    assert result.returncode == 1 and not failed.exists(), result
    assert not list(root.glob(".pg-lisp-initdb-*"))


def check_sync_config(node, port):
    pid = node.is_running().pid
    names = "FIRST 1 (missing_peer)"
    show = "SHOW synchronous_standby_names"
    assert node.config.set_synchronous_standby_names(names) is True
    FOLLOW["wait_for"](lambda: FOLLOW["query"](port, show) == [(names,)])
    assert node.config.set_synchronous_standby_names(names) is None
    assert node.config.set_synchronous_standby_names(None) is True
    FOLLOW["wait_for"](lambda: FOLLOW["query"](port, show) == [("",)])
    assert node.is_running().pid == pid
    print("Patroni 4.1.5 synchronous standby names set, no-op and clear PASS", flush=True)


def check_replication_role(node, replication, rewind):
    def connect(role):
        dsn = {**node.connection_pool.conn_kwargs, "user": role["username"],
               "password": role["password"]}
        return psycopg2.connect(connection_factory=PhysicalReplicationConnection, **dsn)

    with closing(connect(replication)) as connection, connection.cursor() as cursor:
        cursor.execute("IDENTIFY_SYSTEM")
        assert cursor.fetchone()[0] == node.controldata()["Database system identifier"]
        rows = node.query("SELECT r.rolname FROM pg_catalog.pg_stat_get_activity(%s) a "
                          "JOIN pg_catalog.pg_roles r ON r.oid = a.usesysid", connection.get_backend_pid())
        assert rows == [(replication["username"],)]
        try:
            cursor.execute("SELECT current_user")
        except psycopg2.errors.FeatureNotSupported:
            pass
        else:
            raise AssertionError("physical replication accepted SQL")
        for name in ("wal_segment_size", "data_directory_mode"):
            cursor.execute("SHOW " + name)
            assert cursor.fetchone()[0]
        try:
            cursor.execute("SHOW primary_conninfo")
        except psycopg2.errors.InsufficientPrivilege:
            pass
        else:
            raise AssertionError("physical replication exposed a private setting")
    try:
        with closing(connect({**replication, "password": "incorrect-fixture"})):
            raise AssertionError("physical replication accepted an incorrect password")
    except psycopg2.OperationalError as error:
        assert "password authentication failed" in str(error), error
    try:
        with closing(connect(rewind)):
            raise AssertionError("SQL grants alone authorized a native physical image connection")
    except psycopg2.OperationalError as error:
        assert "permission denied to start WAL sender" in str(error), error
    node.query("ALTER ROLE rewind_login NATIVE_REWIND")
    with closing(connect(rewind)) as connection, connection.cursor() as cursor:
        cursor.execute("IDENTIFY_SYSTEM")
        assert cursor.fetchone()[0] == node.controldata()["Database system identifier"]
        try:
            cursor.execute("DROP_REPLICATION_SLOT forbidden_probe")
        except psycopg2.errors.InsufficientPrivilege:
            pass
        else:
            raise AssertionError("rewind account accepted a general slot command")
    print("Patroni 4.1.5 named physical replication role admission PASS", flush=True)


def check_post_bootstrap(node, root, replication, rewind):
    hook = root / "post bootstrap"
    marker = root / "post bootstrap ran"
    psql = shlex.quote(shutil.which("psql"))
    hook.write_text(f'#!/bin/sh\nset -eu\n{psql} -XAtw "$1" -c "SELECT current_user" > '
                    f'{shlex.quote(str(marker))}\n')
    hook.chmod(0o700)
    task = CriticalTask()
    result = node.bootstrap.post_bootstrap({"post_bootstrap": shlex.quote(str(hook))}, task)
    assert marker.read_text().strip() == "postgres"
    for role in (replication, rewind):
        rows = node.query("SELECT rolname FROM pg_catalog.pg_authid WHERE rolname = %s", role["username"])
        assert rows == [(role["username"],)]
    check_replication_role(node, replication, rewind)
    assert result is True
    FOLLOW["control"](node.is_running(), root / "target.sock",
                      '(assert (equal (sort (copy-list (pg::pg-role-function-grants '
                      '(pg::authentication-committed-role "rewind_login"))) #\'string<) '
                      '\'(:ls-dir :read-binary-file :read-binary-file-range :stat-file)))')

    dsn = {**node.connection_pool.conn_kwargs, "user": rewind["username"], "password": rewind["password"]}
    psql_command = ["psql", "-XAtw", "-v", "ON_ERROR_STOP=1", "-h", dsn["host"],
                    "-p", str(dsn["port"]), "-U", dsn["user"], "-d", "postgres", "-f", "-"]
    for sql, allowed in (("SELECT current_user", True),
                         ("SELECT pg_terminate_backend(1)", False)):
        result = subprocess.run(psql_command, input=f"{sql}\n\\bind\n\\g\n",
                                env={**os.environ, "PGPASSWORD": dsn["password"]},
                                text=True, capture_output=True, timeout=15)
        if allowed:
            assert result.returncode == 0 and result.stdout.strip() == rewind["username"], result
        else:
            assert result.returncode != 0 and "outside the rewind SQL scope" in result.stderr, result
    with closing(psycopg2.connect(**dsn)) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT current_user")
        assert cursor.fetchone() == (rewind["username"],)
        cursor.execute("SELECT rolreplication, rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user")
        assert cursor.fetchone() == (False, False)
        cursor.execute("SELECT pg_catalog.pg_read_binary_file('PG_VERSION')")
        assert bytes(cursor.fetchone()[0]).strip().isdigit()
        cursor.execute("SELECT size FROM pg_catalog.pg_stat_file('PG_VERSION', false)")
        assert cursor.fetchone()[0] > 0
        cursor.execute("SELECT NOT pg_catalog.pg_is_in_recovery()"
                       " AND ('x' || pg_catalog.substr(pg_catalog.pg_walfile_name("
                       " pg_catalog.pg_current_wal_lsn()), 1, 8))::bit(32)::int = timeline_id"
                       " FROM pg_catalog.pg_control_checkpoint()")
        assert isinstance(cursor.fetchone()[0], bool)
        for statement in ("SELECT * FROM pg_catalog.pg_authid", "SELECT pg_terminate_backend(1)",
                          "SELECT (SELECT pg_reload_conf())", "CREATE TABLE forbidden (id integer)"):
            try:
                cursor.execute(statement)
            except psycopg2.errors.InsufficientPrivilege:
                connection.rollback()
            else:
                raise AssertionError(f"rewind role accepted {statement}")
    print("Patroni 4.1.5 post-bootstrap hook and SCRAM roles PASS", flush=True)


def check_bootstrap(root, precreated, password=None, post_bootstrap=False):
    root.mkdir(mode=0o700)
    data = root / "fresh pgdata"
    if precreated:
        data.mkdir(mode=0o700)
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    config = FOLLOW["configuration"](data, port, root, major_version=160000)
    initdb = ["no-locale", {"encoding": "UTF8"}]
    if password:
        config["authentication"]["superuser"]["password"] = password
        config["pg_hba"] = [line.replace(" trust", " scram-sha-256") for line in config["pg_hba"]]
        initdb.append({"auth-host": "scram-sha-256"})
    if post_bootstrap:
        replication = {"username": "standby_login", "password": "standby-fixture"}
        rewind = {"username": "rewind_login", "password": "rewind-fixture"}
        config["authentication"].update(replication=replication, rewind=rewind)
    node = Postgresql(config, Null())
    start = node.start
    offline_identity = []

    def check_before_start(*args, **kwargs):
        facts = node.controldata()
        assert facts["Database cluster state"] == "shut down", facts
        assert not (data / "postmaster.pid").exists()
        assert (data / "PG_VERSION").stat().st_mode & 0o777 == 0o600
        if password:
            assert (data / "global/pg_bootstrap_scram").stat().st_mode & 0o777 == 0o600
        offline_identity.append(facts["Database system identifier"])
        return start(*args, **kwargs)

    node.start = check_before_start

    def query(statement):
        if not password:
            return FOLLOW["query"](port, statement)
        with closing(psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres",
                                     password=password, connect_timeout=5)) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(statement)
                return cursor.fetchall() if cursor.description else None

    try:
        assert node.bootstrap.bootstrap({"initdb": initdb})
        assert len(offline_identity) == 1
        assert query("SELECT pg_is_in_recovery(), current_user") == [(False, "postgres")]
        if post_bootstrap:
            query("BEGIN; CREATE ROLE rollback_probe LOGIN; ROLLBACK")
            assert query("SELECT rolname FROM pg_authid WHERE rolname = 'rollback_probe'") == []
            query("CREATE ROLE commit_probe LOGIN")
            assert query("SELECT rolname FROM pg_authid WHERE rolname = 'commit_probe'") == [("commit_probe",)]
            check_post_bootstrap(node, root, replication, rewind)
        query("CREATE TABLE fresh_primary (id integer); INSERT INTO fresh_primary VALUES (42)")
        assert query("SELECT id FROM fresh_primary") == [(42,)]
        assert node.controldata()["Database system identifier"] == offline_identity[0]
        assert (data / "PG_VERSION").is_file() and not list(root.glob(".pg-lisp-initdb-*"))
        retry = subprocess.run([BIN / "initdb", data], capture_output=True, timeout=20)
        assert retry.returncode == 1
        assert query("SELECT id FROM fresh_primary") == [(42,)]
        if not precreated and not password:
            check_sync_config(node, port)
    finally:
        node.connection_pool.close()
        process = node.is_running()
        if process:
            process.send_signal(signal.SIGINT)
            process.wait(timeout=45)
    assert node.controldata()["Database cluster state"] == "shut down"
    print("Patroni 4.1.5 native initdb, primary start, write and clean shutdown PASS", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--post-bootstrap", action="store_true", help="exercise stock Patroni role provisioning")
    args = parser.parse_args()
    assert importlib.metadata.version("patroni") == "4.1.5"
    assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
    with tempfile.TemporaryDirectory(prefix="pg-patroni-bootstrap-") as path:
        root = Path(path)
        if args.post_bootstrap:
            check_bootstrap(root / "post-bootstrap", precreated=False,
                            password="patroni-bootstrap-fixture", post_bootstrap=True)
            return
        check_rejected_targets(root)
        check_bootstrap(root / "absent", precreated=False)
        check_bootstrap(root / "empty", precreated=True)
        check_bootstrap(root / "scram", precreated=False, password="patroni-bootstrap-fixture")


if __name__ == "__main__":
    main()
