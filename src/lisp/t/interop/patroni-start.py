"""Exercise real Patroni config writing and direct process startup, without HA."""
import ctypes
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

import psutil
import psycopg2
from patroni.collections import CaseInsensitiveDict
from patroni.postgresql.config import ConfigHandler, ConfigWriter
from patroni.postgresql.connection import ConnectionPool
from patroni.postgresql.postmaster import PostmasterProcess
from patroni.utils import get_postgres_version


def wait_for(function, seconds=30):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            result = function()
            if result:
                return result
        except (OSError, psycopg2.Error) as error:
            last = error
        time.sleep(0.1)
    raise AssertionError("timeout: " + repr(last))


def check_named_connection(connection, port):
    pool = ConnectionPool()
    pool.conn_kwargs = dict(host="127.0.0.1", port=port, user="postgres", dbname="postgres")
    named = pool.get("config")
    timers = []
    blockers = []
    try:
        assert named.query("SELECT current_setting('statement_timeout'), "
                           "current_setting('pg_stat_statements.track')") == [("2s", "none")]
        assert named.server_version == 200000
        assert named.query("SELECT source FROM pg_settings WHERE name='statement_timeout'") == [("client",)]
        assert named.query("SELECT name FROM pg_settings WHERE name='pg_stat_statements.track'") == []
        named.query("BEGIN")
        named.query("SET statement_timeout='40ms'")
        named.query("ROLLBACK")
        assert named.query("SHOW statement_timeout") == [("2s",)]
        named.query("BEGIN")
        named.query("SELECT set_config('test.transient', 'changed', false)")
        named.query("ROLLBACK")
        assert named.query("SELECT current_setting('test.transient')") == [("",)]
        named.query("SET statement_timeout='100ms'")
        named.query("SET statement_timeout TO DEFAULT")
        assert named.query("SELECT setting,source FROM pg_settings WHERE name='statement_timeout'") == [("2000", "client")]
        named.query("SET statement_timeout='100ms'")
        named.query("RESET ALL")
        assert named.query("SHOW statement_timeout") == [("2s",)]
        first = named.get()
        named.close()
        assert named.query("SELECT 1") == [(1,)] and first is not named.get()
        for options, message in (("-c unknown_setting=1", "unknown_setting"),
                                 ("-c statement_timeout=-1", "statement_timeout"),
                                 ("-c port=5434", "port")):
            try:
                rejected = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                            dbname="postgres", options=options)
            except psycopg2.Error as error:
                assert message in str(error) and "FATAL" in str(error)
            else:
                rejected.close()
                raise AssertionError("invalid startup setting accepted: " + options)
        short = pool.get("short", {"options": "-c statement_timeout=60"})
        assert short.query("SHOW search_path") == [("pg_catalog",)]
        # Synthetic writer workload; Patroni's own queries stay in pg_catalog.
        short.query("SET search_path=public")
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE timeout_probe(id int PRIMARY KEY, v int)")
            cursor.execute("INSERT INTO timeout_probe VALUES (1,0),(2,0)")
            cursor.execute("BEGIN; UPDATE timeout_probe SET v=1 WHERE id=1")
        started = time.monotonic()
        try:
            short.query("UPDATE timeout_probe SET v=2 WHERE id=1")
        except psycopg2.Error as error:
            assert error.pgcode == "57014" and "statement timeout" in str(error)
        else:
            raise AssertionError("blocked update ignored statement timeout")
        assert time.monotonic() - started < 2
        with connection.cursor() as cursor:
            cursor.execute("ROLLBACK")
        assert short.query("SELECT 1") == [(1,)]
        # Idle time is outside the next statement's deadline.
        time.sleep(0.1)
        assert short.query("SELECT 1") == [(1,)]
        # Each statement gets a new budget, even within one simple-query frame.
        short.query("SET statement_timeout='1s'")
        for row in (1, 2):
            blocker = psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres")
            blocker.autocommit = True
            blockers.append(blocker)
            with blocker.cursor() as cursor:
                cursor.execute("BEGIN; UPDATE timeout_probe SET v=3 WHERE id=%s", (row,))

        def release(blocker):
            with blocker.cursor() as cursor:
                cursor.execute("ROLLBACK")

        for delay, blocker in zip((0.6, 1.2), blockers):
            timer = threading.Timer(delay, release, (blocker,))
            timers.append(timer)
            timer.start()
        with short.get().cursor() as cursor:
            cursor.execute("UPDATE timeout_probe SET v=4 WHERE id=1; UPDATE timeout_probe SET v=4 WHERE id=2")
        assert short.query("SELECT 1") == [(1,)]
        print("patroni-start: NamedConnection options/reset/reconnect/timeouts PASS", flush=True)
    finally:
        for timer in timers:
            timer.cancel()
            timer.join()
        for blocker in blockers:
            blocker.close()
        with connection.cursor() as cursor:
            cursor.execute("ROLLBACK")
        pool.close()


def check_timeout_reload_gate(base, root, port):
    conn = psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres",
                           options="-c statement_timeout=60")
    conn.autocommit = True
    control = [os.environ.get("SBCL", "sbcl"), "--script", str(base / "t/control-client.lisp"),
               "unix", str(root / "control.sock")]

    def evaluate(expression):
        output = subprocess.check_output(control, input=(expression + "\n(quit)\n").encode(), timeout=15)
        assert b";; error:" not in output, output
        return output

    result = []
    thread = None
    ready, waiting_file = root / "timeout-gate.ready", root / "timeout-gate.waiting"
    start_file = root / "timeout-gate.start"
    release_file, done, failed = root / "timeout-gate.release", root / "timeout-gate.done", root / "timeout-gate.failed"
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_backend_pid()")
            pid = cursor.fetchone()[0]
        # The control evaluator itself owns normal admission. A separate task
        # thread must own quiescence; file barriers remain usable while it does.
        evaluate('(let ((b (lookup-backend ' + str(pid) + '))) '
                 '(sb-thread:make-thread (lambda () '
                 '(flet ((mark (path text) (with-open-file (s path :direction :output '
                 ':if-exists :supersede :if-does-not-exist :create) (write-string text s)))) '
                 '(handler-case '
                 '(progn (loop repeat 1000 until (probe-file ' + json.dumps(str(start_file)) + ') '
                 'do (sleep 0.01)) (with-live-operation-quiescence '
                 '(mark ' + json.dumps(str(ready)) + ' "ready") '
                 '(loop repeat 1000 until (probe-file ' + json.dumps(str(release_file)) + ') '
                 'do (sb-thread:with-mutex (*live-operation-lock*) '
                 '(when (backend-executing b) (assert (null (backend-statement-deadline b))) '
                 '(mark ' + json.dumps(str(waiting_file)) + ' "waiting"))) (sleep 0.01))) '
                 '(mark ' + json.dumps(str(done)) + ' "done")) '
                 '(error (e) (mark ' + json.dumps(str(failed)) + ' (princ-to-string e))))))))')
        start_file.write_text("start")

        def barrier(path):
            if failed.exists():
                raise AssertionError(failed.read_text())
            return path.exists()

        wait_for(lambda: barrier(ready))

        def query():
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result.append(cursor.fetchone())
            except Exception as error:
                result.append(error)

        thread = threading.Thread(target=query)
        thread.start()

        wait_for(lambda: barrier(waiting_file))
        time.sleep(0.15)  # Exceeds 60 ms while still outside statement execution.
    finally:
        start_file.write_text("start")
        release_file.write_text("release")
        if thread:
            thread.join(3)
        conn.close()
    wait_for(lambda: barrier(done))
    assert result == [(1,)], result
    print("patroni-start: timeout excludes reload admission wait PASS", flush=True)


def main():
    base, root, port = Path(sys.argv[1]), Path(sys.argv[2]), int(sys.argv[3])
    assert importlib.metadata.version("patroni") == "4.1.5"
    assert importlib.metadata.version("psycopg2-binary") == "2.9.11"
    # Reap the detached server in container test environments too. This is local
    # test process cleanup; no controller, DCS or failover policy runs here.
    assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
    executable = str(base / "pg-lisp.sh")
    assert get_postgres_version(bin_name=executable) == "20.0"
    configuration = root / "config files"
    configuration.mkdir(mode=0o700)
    data = root / "db 'one'"
    data.mkdir(mode=0o700)
    conf = configuration / "postgresql.conf"
    included = configuration / "settings"
    include_dir = configuration / "conf.d"
    include_dir.mkdir()
    auto = data / "postgresql.auto.conf"

    def write_base(host):
        with ConfigWriter(str(included)) as writer:
            writer.write_param("port", port + 3)
            writer.write_param("listen_addresses", host)
            writer.write_param("checkpoint_timeout", "30s")
            writer.write_param("max_slot_wal_keep_size", -1)

    def write_auto(timeout, extra=None):
        with ConfigWriter(str(auto)) as writer:
            writer.write_param("port", port + 2)
            writer.write_param("checkpoint_timeout", timeout)
            writer.write_param("max_wal_size", "5GB")
            if extra:
                writer.write_param(*extra)

    write_base("127.0.0.1")
    with ConfigWriter(str(include_dir / "10.conf")) as writer:
        writer.write_param("max_wal_size", "2GB")
    with ConfigWriter(str(include_dir / "20.conf")) as writer:
        writer.write_param("max_wal_size", "3GB")
    (include_dir / ".hidden.conf").write_text("unsupported_hidden = 'ignored'\n")
    (include_dir / "ignored.txt").write_text("unsupported_extension = 'ignored'\n")
    with ConfigWriter(str(conf)) as writer:
        writer.write_param("include", "settings")
        writer.write_param("include_dir", "conf.d")
        writer.write_param("include_if_exists", "missing")
        writer.write_param("max_wal_size", "4GB")
        writer.write_param("data_directory", str(data))
    write_auto("60s")
    options = ["-c", "port=" + str(port), "--max_wal_size=6GB",
               "--control-socket=" + str(root / "control.sock")]
    process = None
    connection = None
    try:
        process = PostmasterProcess.start(executable, str(data), str(conf), options)
        assert process is not None

        def connect():
            if process.status() == psutil.STATUS_ZOMBIE:
                raise AssertionError("server exited before SQL readiness")
            conn = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                   dbname="postgres", connect_timeout=1)
            conn.autocommit = True
            return conn

        connection = wait_for(connect)
        subprocess.run([sys.executable, str(base / "t/interop/wal-sql.py"), str(port)], check=True)
        subprocess.run([sys.executable, str(base / "t/interop/string-sql.py"), str(port)], check=True)
        subprocess.run([sys.executable, str(base / "t/interop/bit-sql.py"), str(port)], check=True)
        subprocess.run([sys.executable, str(base / "t/interop/wal-filename-sql.py"), str(port)], check=True)
        check_named_connection(connection, port)
        check_timeout_reload_gate(base, root, port)
        subprocess.run([sys.executable, str(base / "t/interop/patroni-history.py"),
                        str(data), str(port)], check=True)

        class SqlPeer:
            """Supply the real SQL connection without starting Patroni's HA loop."""
            major_version = 200000

            def query(self, sql, *parameters):
                with connection.cursor() as cursor:
                    cursor.execute(sql, parameters)
                    return cursor.fetchall()

            def is_primary(self):
                return True

        # Exercise the pinned methods unchanged. Constructor/full generated
        # configurations and recovery parameters are separate acceptance gates.
        handler = ConfigHandler.__new__(ConfigHandler)
        handler._postgresql = SqlPeer()
        handler._postgresql_conf = str(conf)
        handler._server_parameters = CaseInsensitiveDict(
            {name: option[0] for name, option in handler.CMDLINE_OPTIONS.items()})
        handler._server_parameters.update(port=port, listen_addresses="127.0.0.1")
        pg_values = handler._get_pg_settings(["PORT", "CHECKPOINT_TIMEOUT", "max_wal_size",
                                              "missing.setting"])
        assert set(pg_values) == {"port", "checkpoint_timeout", "max_wal_size"}
        assert pg_values["checkpoint_timeout"][1:5] == ("60", "s", "integer", "sighup")
        assert handler._get_pg_settings([]) == {}
        handler.load_current_server_parameters()
        assert handler._server_parameters["max_wal_size"] == "6GB"
        assert handler._server_parameters["port"] == port
        assert "checkpoint_timeout" not in handler._server_parameters
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_setting('CHECKPOINT_TIMEOUT'), "
                           "current_setting('missing.setting', true), "
                           "current_setting(NULL), current_setting('port', NULL), "
                           "current_setting('missing.setting', 'true'), "
                           "pg_catalog.lower('PORT ÄΣ')")
            assert cursor.fetchone() == ("1min", None, None, None, None, "port ÄΣ")
            cursor.execute("SELECT 1 = ANY('{1,2}'), 'r'::\"char\" = ANY('{rel}'), "
                           "'port' != ALL('{}')")
            assert cursor.fetchone() == (True, True, True)
            try:
                cursor.execute("SELECT 1 = ANY('{1,bad}')")
            except psycopg2.Error as error:
                assert error.pgcode == "22P02"
            else:
                raise AssertionError("invalid array element hidden by earlier match")
            try:
                cursor.execute("SELECT current_setting('missing.setting')")
            except psycopg2.Error as error:
                assert error.pgcode == "42704"
            else:
                raise AssertionError("unknown current_setting silently accepted")

        def show(name):
            with connection.cursor() as cursor:
                cursor.execute("SHOW " + name)
                return cursor.fetchone()[0]

        assert show("port") == str(port)
        assert show("listen_addresses") == "127.0.0.1"
        assert show("checkpoint_timeout") == "1min"
        assert show("max_wal_size") == "6GB"
        assert show("max_slot_wal_keep_size") == "-1"
        assert Path(show("data_directory")) == data
        assert Path(show("config_file")) == conf
        def settings():
            with connection.cursor() as cursor:
                cursor.execute("SELECT name, setting, unit, vartype, context, source, "
                               "sourcefile, sourceline, pending_restart "
                               "FROM pg_catalog.pg_settings")
                return {row[0]: row[1:] for row in cursor.fetchall()}

        values = settings()
        assert values["checkpoint_timeout"] == ("60", "s", "integer", "sighup",
                                                 "configuration file", str(auto), 4, False)
        assert values["max_wal_size"][:6] == ("6144", "MB", "integer", "sighup",
                                              "command line", None)
        for name in ("data_directory", "config_file"):
            assert values[name][4:7] == ("override", None, None)
        with connection.cursor() as cursor:
            cursor.execute("SELECT attnum FROM pg_catalog.pg_attribute "
                           "WHERE attrelid=12109 AND attname='context'")
            assert cursor.fetchone() == (7,)
            cursor.execute("SELECT relnatts FROM pg_catalog.pg_class WHERE oid=12109")
            assert cursor.fetchone() == (17,)
        for name in ("port", "listen_addresses", "data_directory", "config_file",
                     "checkpoint_timeout", "max_wal_size", "max_slot_wal_keep_size"):
            for statement in ("SET " + name + " TO DEFAULT", "RESET " + name,
                              "SELECT set_config('" + name + "', NULL, false)"):
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(statement)
                except psycopg2.Error as error:
                    assert error.pgcode == "55P02", (statement, error.pgcode)
                else:
                    raise AssertionError("session changed server setting: " + statement)
        subprocess.run([sys.executable, str(base / "t/interop/patroni-process.py"),
                        str(data), str(process.pid), str(port), "check"], check=True)
        write_base("localhost")
        write_auto("90s")
        with conf.open("a") as stream:
            stream.write("data_directory = '" + str(root / "ignored-on-reload") + "'\n")
        process.send_signal(signal.SIGHUP)
        wait_for(lambda: show("checkpoint_timeout") == "90s")
        assert handler._get_pg_settings(["checkpoint_timeout"])["checkpoint_timeout"][1] == "90"
        assert handler._postgresql.query("SELECT current_setting('checkpoint_timeout')") == [("90s",)]
        assert show("listen_addresses") == "127.0.0.1"
        assert settings()["listen_addresses"][0] == "127.0.0.1"
        assert settings()["listen_addresses"][-1] is True
        assert Path(show("data_directory")) == data
        assert settings()["data_directory"][-1] is False
        assert show("port") == str(port)
        assert show("max_wal_size") == "6GB"
        sbcl = os.environ.get("SBCL", "sbcl")
        control = [sbcl, "--script", str(base / "t/control-client.lisp"),
                   "unix", str(root / "control.sock")]
        response = subprocess.check_output(
            control,
            input=b'(assert (getf (gethash "listen_addresses" *startup-server-settings*) :pending-restart))\n(quit)\n')
        assert b";; error:" not in response
        log = root / "driver.log"
        before = log.read_text(errors="replace").count("reload failed:")
        write_auto("120s", ("unsupported_parameter", "must fail"))
        process.send_signal(signal.SIGHUP)
        wait_for(lambda: log.read_text(errors="replace").count("reload failed:") > before)
        assert show("checkpoint_timeout") == "90s"
        write_base("127.0.0.1")
        write_auto("120s")
        process.send_signal(signal.SIGHUP)
        wait_for(lambda: show("checkpoint_timeout") == "2min")
        with connection.cursor() as cursor:
            cursor.execute("SELECT COALESCE(sum(1),0), 'x'||substr('ab',1,1), ('x'||substr('FFFFFFFF0000000000000001',1,8))::bit(32)::int")
            assert cursor.fetchone() == (1, "xa", -1)
        print("patroni-start: aggregate definitions survive live reload PASS", flush=True)
        for argument in ("--typo=1", "unexpected"):
            rejected = root / ("rejected-" + str(len(argument)))
            result = subprocess.run([executable, "-D", str(rejected), argument],
                                    capture_output=True, timeout=15)
            assert result.returncode == 1 and not rejected.exists()
        print("patroni-start: direct start/config/includes/precedence/reload PASS", flush=True)
    finally:
        if connection:
            connection.close()
        if process:
            process.signal_stop("fast")

            def reap():
                pid, status = os.waitpid(process.pid, os.WNOHANG)
                return status + 1 if pid else False

            try:
                status = wait_for(reap, 15) - 1
                assert os.waitstatus_to_exitcode(status) == 0
            except Exception:
                process.kill()
                os.waitpid(process.pid, 0)
                raise
    assert not (data / "postmaster.pid").exists()
    if os.environ.get("C_PG_BINDIR"):
        c_bin = Path(os.environ["C_PG_BINDIR"])
        c_data = root / "c-reference"
        subprocess.run([str(c_bin / "initdb"), "-D", str(c_data), "-U", "postgres",
                        "-A", "trust", "--no-locale"], check=True, stdout=subprocess.DEVNULL)
        for name, value, expected in [
            ("checkpoint_timeout", "30.5001s", 30),
            ("checkpoint_timeout", "1.5min", 90),
            ("max_wal_size", "1.4998MB", 2),
            ("max_wal_size", "1.1GB", 1126),
            ("port", "0x1538", 5432),
            ("port", "5432.5", 5432),
        ]:
            output = subprocess.check_output([str(c_bin / "postgres"), "-D", str(c_data),
                                              "-C", name, "-c", name + "=" + value])
            assert int(output) == expected, (name, value, output)
        print("patroni-start: pinned C numeric oracle PASS", flush=True)


if __name__ == "__main__":
    main()
