"""Keep promotion pending while its SQL caller returns, aborts or disappears."""
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg2


def connect(port, name="sql-promotion-observer"):
    connection = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                  dbname="postgres", application_name=name, connect_timeout=3)
    connection.autocommit = True
    return connection


def query(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall() if cursor.description else None


def scalar(connection, sql):
    return query(connection, sql)[0][0]


def wait_until(predicate):
    deadline = time.monotonic() + 25
    while not predicate():
        assert time.monotonic() < deadline, "SQL promotion fault barrier timed out"
        time.sleep(0.025)


def control(base, root, expression):
    result = subprocess.run([os.environ.get("SBCL", "sbcl"), "--script", str(base / "t/control-client.lisp"),
                             "unix", str(root / "target.sock")], input=expression + "\n(quit)\n",
                            text=True, capture_output=True, timeout=30, check=True)
    assert ";; error:" not in result.stdout + result.stderr, result.stdout + result.stderr
    return result.stdout


class NativeWorker:
    def __init__(self, base, root, target_pid, backend_pid, case):
        self.base, self.root, self.target_pid = base, root, target_pid
        self.data = root / "copy"
        control(base, root, f'(load "{base}/t/promotion-sql-native.lisp")')
        mode = ":handoff-cancel" if case == "handoff_cancel" else ":normal"
        control(base, root, f'(sql-promotion-test-install #p"{root}/" {backend_pid} {mode})')

    def marker(self, name):
        path = self.root / name
        return path.exists() and int(path.read_text()) == self.target_pid

    def wait_requested(self):
        wait_until(lambda: self.marker("sql-worker-held"))

    def wait_waiter(self, reader, observer):
        wait_until(lambda: self.marker("sql-parked"))

    def reload(self):
        subprocess.run([sys.executable, str(self.base / "t/interop/signal-owned-server.py"),
                        str(self.target_pid), str(self.data), "HUP"], check=True, timeout=5)
        wait_until(lambda: self.marker("sql-reload-backed-off"))

    def release(self):
        (self.root / "sql-worker-release").touch()

    def verify(self, parks, retained):
        control(self.base, self.root, f'(assert (sql-promotion-test-verify {parks} {"t" if retained else "nil"}))')

    def close(self):
        self.release()
        control(self.base, self.root, "(sql-promotion-test-restore)")


class CWorker:
    def __init__(self, root):
        self.root, self.data = root, root / "standby"

    def wait_requested(self):
        wait_until(lambda: (self.root / "c-worker-held").is_file())

    def wait_waiter(self, reader, observer):
        wait_until(lambda: scalar(observer, f"SELECT wait_event FROM pg_stat_activity WHERE pid={reader.get_backend_pid()}") == "Promote")

    def reload(self):
        pass

    def release(self):
        (self.root / "c-worker-release").touch()

    def verify(self, parks, retained):
        pass

    def close(self):
        self.release()


def unsafe_batch(base, root, source_port, target_port, target_pid):
    source, reader, observer = connect(source_port), connect(target_port), connect(target_port)
    worker = None
    try:
        reader.autocommit = False
        reader.set_session(isolation_level="REPEATABLE READ")
        assert query(reader, "SELECT id,label FROM promotion_sql_items ORDER BY id") == [(1, "before")]
        worker = NativeWorker(base, root, target_pid, reader.get_backend_pid(), "unsafe_batch")
        query(source, "CREATE TABLE promotion_catalog_barrier (id integer)")
        ddl_position = scalar(source, "SELECT pg_current_wal_flush_lsn()")
        query(source, "INSERT INTO promotion_sql_items VALUES (2,'blocked')")
        position = scalar(source, "SELECT pg_current_wal_flush_lsn()")
        wait_until(lambda: scalar(observer, f"SELECT pg_last_wal_receive_lsn() >= '{position}'::pg_lsn"))
        assert scalar(observer, f"SELECT pg_last_wal_replay_lsn() < '{ddl_position}'::pg_lsn")
        worker.release()
        assert scalar(reader, "SELECT pg_promote(true,1)") is False
        assert any("server did not promote within 1 second" in notice for notice in reader.notices)
        assert scalar(observer, "SELECT pg_is_in_recovery()") is True
        assert scalar(observer, f"SELECT pg_last_wal_replay_lsn() < '{ddl_position}'::pg_lsn")
        assert query(reader, "SELECT id,label FROM promotion_sql_items ORDER BY id") == [(1, "before")]
        worker.verify(1, True)
        reader.commit()
        wait_until(lambda: scalar(observer, "SELECT pg_is_in_recovery()") is False)
        assert query(observer, "SELECT id,label FROM promotion_sql_items ORDER BY id") == [(1, "before"), (2, "blocked")]
        assert scalar(observer, "SELECT count(*) FROM promotion_catalog_barrier") == 0
        query(observer, "INSERT INTO promotion_catalog_barrier VALUES (3)")
        print("promotion-sql-faults: unsafe batch defers past deadline, reader release completes accepted request PASS", flush=True)
    finally:
        if worker:
            worker.release()
        reader.close()
        if worker:
            worker.close()
        source.close()
        observer.close()


def run(case, root, port, native=None):
    reader, observer = connect(port, "sql-promotion-waiter"), connect(port)
    backend_pid = reader.get_backend_pid()
    worker, pool = None, ThreadPoolExecutor(max_workers=1)
    retained = case == "timeout"
    try:
        assert scalar(observer, "SELECT pg_is_in_recovery()") is True
        if retained:
            reader.autocommit = False
            reader.set_session(isolation_level="REPEATABLE READ")
            assert query(reader, "SELECT id,label FROM promotion_sql_items ORDER BY id") == [(1, "before")]
        worker = NativeWorker(*native, backend_pid, case) if native else CWorker(root)
        if case == "request_file":
            path = worker.data / "promote"
            path.mkdir()
            try:
                query(reader, "SELECT pg_promote(false,1)")
                raise AssertionError("directory promote request unexpectedly succeeded")
            except psycopg2.Error as failure:
                assert failure.pgcode == "42809", failure
            finally:
                path.rmdir()
            assert scalar(observer, "SELECT pg_is_in_recovery()") is True
        if case in ("timeout", "cancel", "disconnect"):
            future = pool.submit(scalar, reader, f"SELECT pg_promote(true,{1 if retained else 60})")
            worker.wait_waiter(reader, observer)
            worker.wait_requested()
            if retained:
                worker.reload()
                assert future.result(timeout=10) is False
                assert any("server did not promote within 1 second" in notice for notice in reader.notices)
            elif case == "cancel":
                reader.cancel()
                try:
                    future.result(timeout=10)
                    raise AssertionError("cancelled promotion waiter succeeded")
                except psycopg2.Error as failure:
                    assert failure.pgcode == "57014", failure
            else:
                with socket.fromfd(reader.fileno(), socket.AF_INET, socket.SOCK_STREAM) as duplicate:
                    duplicate.shutdown(socket.SHUT_RDWR)
                try:
                    future.result(timeout=10)
                    raise AssertionError("disconnected promotion waiter succeeded")
                except psycopg2.OperationalError:
                    pass
        elif case in ("statement_error", "handoff_cancel"):
            try:
                query(reader, "SELECT pg_promote(false,1),1/(id-1) FROM promotion_sql_items"
                      if case == "statement_error" else "SELECT pg_promote(true,10)")
                raise AssertionError("promotion fault unexpectedly succeeded")
            except psycopg2.Error as failure:
                assert failure.pgcode == ("22012" if case == "statement_error" else "57014"), failure
        else:
            if case == "rollback":
                query(reader, "BEGIN")
            assert scalar(reader, "SELECT pg_promote(false,1)") is True
            if case == "rollback":
                query(reader, "ROLLBACK")
                assert reader.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_IDLE
        worker.wait_requested()
        if native:
            assert (worker.data / "promote").is_file()
        assert scalar(observer, "SELECT pg_is_in_recovery()") is True
        worker.release()
        wait_until(lambda: scalar(observer, "SELECT pg_is_in_recovery()") is False)
        query(observer, "INSERT INTO promotion_sql_items VALUES (2,'child')")
        if case == "disconnect":
            if native:
                wait_until(lambda: ":GONE" in control(native[0], root,
                           f'(if (lookup-backend {backend_pid}) :alive :gone)'))
            else:
                wait_until(lambda: scalar(observer, f"SELECT count(*) FROM pg_stat_activity WHERE pid={backend_pid}") == 0)
        if retained:
            assert query(reader, "SELECT id,label FROM promotion_sql_items ORDER BY id") == [(1, "before")]
            assert scalar(reader, "SHOW transaction_read_only") == "on"
        worker.verify(1 if case in ("timeout", "cancel", "disconnect") else 0, retained)
        if retained:
            reader.commit()
            assert query(reader, "SELECT id,label FROM promotion_sql_items ORDER BY id") == [(1, "before"), (2, "child")]
            assert scalar(reader, "SHOW transaction_read_only") == "off"
            reader.commit()
        print(f"promotion-sql-faults: {case} independent request, primary writes and cleanup PASS", flush=True)
    finally:
        if worker:
            worker.release()
        reader.close()
        pool.shutdown(wait=True)
        if worker:
            worker.close()
        observer.close()


if __name__ == "__main__":
    case = os.environ["PROMOTION_SQL_CASE"]
    if sys.argv[1] == "--c":
        run(case, Path(sys.argv[2]), int(sys.argv[3]))
    else:
        base, root = Path(sys.argv[1]), Path(sys.argv[2])
        if case == "unsafe_batch":
            unsafe_batch(base, root, *map(int, sys.argv[3:]))
        else:
            run(case, root, int(sys.argv[4]), (base, root, int(sys.argv[5])))
